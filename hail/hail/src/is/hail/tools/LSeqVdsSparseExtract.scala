package is.hail.tools

import is.hail.HailFeatureFlags
import is.hail.annotations.{Region, RegionPool, UnsafeRow}
import is.hail.asm4s.HailClassLoader
import is.hail.backend.{ExecuteContext, OwningTempFileManager}
import is.hail.backend.local.LocalBackend
import is.hail.collection.compat.immutable.ArraySeq
import is.hail.expr.ir.{AbstractMatrixTableSpec, BaseIR, RelationalSpec}
import is.hail.expr.ir.LoweredTableReader.LoweredTableReaderCoercer
import is.hail.expr.ir.lowering.IrMetadata
import is.hail.io.Decoder
import is.hail.io.fs.{CloudStorageConfig, FS, GoogleStorageConfig, RequesterPaysConfig, RouterFS}
import is.hail.linalg.BlockMatrix
import is.hail.types.physical.{PArray, PLocus, PString, PStruct}
import is.hail.types.virtual.MatrixType
import is.hail.utils.{ExecutionTimer, Interval, fatal, using}
import is.hail.variant.{Call, Locus, ReferenceGenome}

import scala.collection.mutable
import scala.io.Source

import java.io.{BufferedOutputStream, DataOutputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{Callable, Executors}

import org.apache.spark.sql.Row

object LSeqVdsSparseExtract {
  private final case class Config(
    vds: String = "",
    chrom: String = "",
    start: Int = 0,
    end: Int = 0,
    vat: Option[String] = None,
    colContig: String = "contig",
    colPosition: String = "position",
    colRef: String = "ref_allele",
    colAlt: String = "alt_allele",
    colAC: String = "gvs_all_ac",
    colAN: String = "gvs_all_an",
    colAF: String = "gvs_all_af",
    colSC: String = "gvs_all_sc",
    vatSummaryColPrefix: Option[String] = None,
    minMAC: Int = 20,
    maxMAF: Double = 1.0,
    outPrefix: Option[String] = None,
    limitRows: Int = 0,
    inspectOnly: Boolean = false,
    sampleList: Option[String] = None,
    sampleIndexList: Option[String] = None,
    sampleCol: Int = 0,
    chunkBp: Option[Int] = None,
    threads: Option[Int] = None,
    gcsRequesterPaysProject: Option[String] = None,
    gcsRequesterPaysBuckets: Option[Set[String]] = None,
  )

  private final case class VariantKey(contig: String, position: Int, ref: String, alt: String)

  private final case class VariantInfo(
    index: Int,
    key: VariantKey,
    ac: Int,
    an: Int,
    af: Double,
    mac: Int,
    maf: Double,
    sc: Int,
    minorAlleleIndex: Int,
  )

  private final case class VatReadResult(
    retainedByAllele: Map[VariantKey, VariantInfo],
    qualifiedRows: Long,
    duplicateRows: Long,
    duplicatedAlleles: Long,
  )

  private final case class ExtractStats(
    rowsRead: Long,
    rowsInInterval: Long,
    retainedVariantRows: Long,
    definedEntries: Long,
    carrierEvents: Long,
  )

  private final case class Chunk(
    index: Int,
    start: Int,
    end: Int,
    queryStart: Int,
    queryEnd: Int,
  )

  private final case class ChunkResult(chunk: Chunk, stats: ExtractStats)

  private final case class VariantCounts(var het: Int = 0, var hom: Int = 0)

  private final class NativeRowStream(
    is: InputStream,
    decoder: Decoder,
    region: Region,
  ) extends AutoCloseable {
    def nextOffset(): Long = {
      val sentinel = decoder.readByte()
      if (sentinel == 0.toByte) 0L
      else if (sentinel == 1.toByte) decoder.readRegionValue(region)
      else fatal(s"unexpected native partition sentinel byte: $sentinel")
    }

    override def close(): Unit = is.close()
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args.toIndexedSeq)
    if (config.vds.isEmpty)
      usage("--vds is required")
    if (config.chrom.isEmpty)
      usage("--chrom is required")
    if (config.start < 1)
      usage("--start is required and must be >= 1")
    if (config.end < 1)
      usage("--end is required and must be >= 1")
    if (config.start < 1 || config.end < config.start)
      usage(s"invalid interval ${config.chrom}:${config.start}-${config.end}")
    if (config.end == Int.MaxValue)
      usage("--end is too large")
    val endExclusive = config.end + 1
    if (config.sampleList.nonEmpty && config.sampleIndexList.nonEmpty)
      usage("only one of --sample-list and --sample-index-list may be specified")
    if (config.sampleCol < 0)
      usage("--sample-col must be >= 0")
    config.chunkBp.foreach { bp =>
      if (bp <= 0)
        usage("--chunk-bp must be > 0")
      val startAligned = (config.start - 1) % bp == 0
      val endAligned = (config.end - config.start + 1) % bp == 0
      if (!startAligned || !endAligned)
        usage(
          s"with --chunk-bp $bp, --start/--end must describe complete physical chunks: " +
            s"start must be chunk_id * chunk_bp + 1 and inclusive end must be start + N * chunk_bp - 1"
        )
    }
    config.threads.foreach { n =>
      if (n <= 0)
        usage("--threads must be > 0")
    }
    if (config.chunkBp.nonEmpty && config.limitRows > 0)
      usage("--limit-rows is not supported with --chunk-bp")
    if (config.chunkBp.nonEmpty && !config.inspectOnly && config.outPrefix.isEmpty)
      usage("--out-prefix is required with --chunk-bp")

    val fs = RouterFS.buildRoutes(cloudStorageConfig(config))
    withExecuteContext(fs) { ctx =>
      val variantPath = variantDataPath(config.vds)
      val spec = RelationalSpec.read(fs, variantPath).asInstanceOf[AbstractMatrixTableSpec]
      printSpec(variantPath, spec, fs)
      val selectedSampleIndices = selectedSampleIndicesFromConfig(ctx, variantPath, spec, config)

      val retained = config.vat match {
        case Some(path) =>
          val result = readVat(fs, path, config)
          println(s"qualified VAT rows: ${result.qualifiedRows}")
          println(s"retained unique VAT alleles: ${result.retainedByAllele.size}")
          println(s"duplicate qualified VAT rows: ${result.duplicateRows}")
          println(s"duplicated qualified VAT alleles: ${result.duplicatedAlleles}")
          result.retainedByAllele
        case None =>
          Map.empty[VariantKey, VariantInfo]
      }

      if (config.inspectOnly) {
        val chunks = config.chunkBp match {
          case Some(bp) => makeChunks(config.start, config.end, bp)
          case None => IndexedSeq(Chunk(0, config.start, endExclusive, config.start, endExclusive))
        }
        chunks.foreach { chunk =>
          val selected = selectedPartitionRange(ctx, spec, config.chrom, chunk.queryStart, chunk.queryEnd)
          val label =
            if (config.chunkBp.nonEmpty)
              s"chunk ${chunk.index} ${chunk.start}-${chunk.end - 1}: "
            else ""
          println(s"${label}selected row/entry partitions: ${selected.start}..${selected.end - 1} (${selected.length})")
        }
        selectedSampleIndices.foreach(indices => println(s"selected samples: ${indices.length}"))
      } else {
        val stats = config.chunkBp match {
          case Some(bp) =>
            scanChunks(ctx, variantPath, spec, config, retained, selectedSampleIndices, makeChunks(config.start, config.end, bp))
          case None =>
            val selected = selectedPartitionRange(ctx, spec, config.chrom, config.start, endExclusive)
            scanVariantData(
              ctx,
              variantPath,
              spec,
              config,
              retained,
              selectedSampleIndices,
              config.start,
              endExclusive,
              config.start,
              endExclusive,
            selected,
            config.outPrefix,
            config.limitRows,
              ctx.theHailClassLoader,
              config.vat.isEmpty,
            )
        }
        println(s"rows read: ${stats.rowsRead}")
        println(s"rows in interval: ${stats.rowsInInterval}")
        println(s"retained variant rows: ${stats.retainedVariantRows}")
        println(s"defined entry elements: ${stats.definedEntries}")
        println(s"carrier events written: ${stats.carrierEvents}")
      }
    }
  }

  private def parseArgs(args: IndexedSeq[String]): Config = {
    var c = Config()
    var i = 0
    def needValue(flag: String): String = {
      if (i + 1 >= args.length)
        usage(s"$flag requires a value")
      i += 1
      args(i)
    }
    while (i < args.length) {
      args(i) match {
        case "--vds" => c = c.copy(vds = needValue(args(i)))
        case "--chrom" => c = c.copy(chrom = needValue(args(i)))
        case "--start" => c = c.copy(start = needValue(args(i)).toInt)
        case "--end" => c = c.copy(end = needValue(args(i)).toInt)
        case "--vat" => c = c.copy(vat = Some(needValue(args(i))))
        case "--col-contig" => c = c.copy(colContig = needValue(args(i)))
        case "--col-position" => c = c.copy(colPosition = needValue(args(i)))
        case "--col-ref" => c = c.copy(colRef = needValue(args(i)))
        case "--col-alt" => c = c.copy(colAlt = needValue(args(i)))
        case "--col-ac" => c = c.copy(colAC = needValue(args(i)))
        case "--col-an" => c = c.copy(colAN = needValue(args(i)))
        case "--col-af" => c = c.copy(colAF = needValue(args(i)))
        case "--col-sc" => c = c.copy(colSC = needValue(args(i)))
        case "--vat-summary-col-prefix" =>
          val prefix = needValue(args(i))
          c = c.copy(
            vatSummaryColPrefix = Some(prefix),
            colAC = prefix + "ac",
            colAN = prefix + "an",
            colAF = prefix + "af",
            colSC = prefix + "sc",
          )
        case "--min-ac" => c = c.copy(minMAC = needValue(args(i)).toInt)
        case "--min-mac" => c = c.copy(minMAC = needValue(args(i)).toInt)
        case "--max-af" => c = c.copy(maxMAF = needValue(args(i)).toDouble)
        case "--max-maf" => c = c.copy(maxMAF = needValue(args(i)).toDouble)
        case "--out-prefix" => c = c.copy(outPrefix = Some(needValue(args(i))))
        case "--limit-rows" => c = c.copy(limitRows = needValue(args(i)).toInt)
        case "--inspect-only" => c = c.copy(inspectOnly = true)
        case "--sample-list" => c = c.copy(sampleList = Some(needValue(args(i))))
        case "--sample-index-list" => c = c.copy(sampleIndexList = Some(needValue(args(i))))
        case "--sample-col" => c = c.copy(sampleCol = needValue(args(i)).toInt)
        case "--chunk-bp" => c = c.copy(chunkBp = Some(needValue(args(i)).toInt))
        case "--threads" => c = c.copy(threads = Some(needValue(args(i)).toInt))
        case "--gcs-requester-pays-project" =>
          c = c.copy(gcsRequesterPaysProject = Some(needValue(args(i))).filter(_.nonEmpty))
        case "--gcs-requester-pays-buckets" =>
          val buckets = needValue(args(i)).split(",").iterator.map(_.trim).filter(_.nonEmpty).toSet
          c = c.copy(gcsRequesterPaysBuckets = Some(buckets).filter(_.nonEmpty))
        case "--help" | "-h" => usage()
        case other => usage(s"unknown argument: $other")
      }
      i += 1
    }
    c
  }

  private def usage(message: String = ""): Nothing = {
    if (message.nonEmpty)
      System.err.println(s"error: $message\n")
    System.err.println(
      """usage: is.hail.tools.LSeqVdsSparseExtract \
        |  --vds PATH --chrom CHROM --start BP --end BP \
        |  [--vat PATH] [--col-contig contig] [--col-position position] \
        |  [--col-ref ref_allele] [--col-alt alt_allele] \
        |  [--col-ac gvs_all_ac] [--col-an gvs_all_an] \
        |  [--col-af gvs_all_af] [--col-sc gvs_all_sc] \
        |  [--vat-summary-col-prefix PREFIX] \
        |  [--min-mac 20] [--max-maf 1.0] [--out-prefix PATH] \
        |  [--limit-rows N] [--inspect-only] [--sample-list PATH] [--sample-index-list PATH] [--sample-col N] \
        |  [--chunk-bp N] [--threads N] \
        |  [--gcs-requester-pays-project PROJECT] [--gcs-requester-pays-buckets BUCKET[,BUCKET...]]
        |
        |VAT filtering:
        |  --start and --end are both inclusive.
        |  AF is derived as AC / AN from --col-ac and --col-an.
        |  A VAT row passes when min(AC, AN - AC) >= min_mac
        |  and min(AC / AN, 1 - AC / AN) <= max_maf.
        |  The AF column is written to the sidecar if present, but is not used for filtering.
        |
        |Sample filtering:
        |  --sample-list is a plain text file. The first whitespace-delimited token
        |  on each non-empty line is interpreted as a VDS sample ID. Output sample_index
        |  values remain original VDS column indexes.
        |  --sample-index-list is a plain text file of original 0-based VDS column
        |  indexes. It bypasses sample-ID matching. Only one of --sample-list and
        |  --sample-index-list may be specified.
        |  --sample-col selects the 0-based whitespace-delimited column to read from
        |  either sample list format. Default: 0.
        |
        |Chunking:
        |  --chunk-bp requires --start/--end to cover complete physical chunks:
        |  start = chunk_id * chunk_bp + 1 and end = start + N * chunk_bp - 1.
        |
        |GCS requester pays:
        |  --gcs-requester-pays-project defaults to GCS_REQUESTER_PAYS_PROJECT,
        |  then GOOGLE_PROJECT, if either environment variable is set.
        |  --gcs-requester-pays-buckets defaults to GCS_REQUESTER_PAYS_BUCKETS.
        |  If buckets are omitted, the requester-pays project is used for all gs:// buckets.
        |
        |Output files when --out-prefix is set:
        |  PATH.events.bin: LSQ1 metadata header, then repeated
        |                   int variant_index, int sample_index, byte dosage, int sc
        |  PATH.variants.tsv: variant_index, contig, position, ref, alt, minor_allele,
        |                     ac, an, af, mac, maf, sc
        |""".stripMargin)
    sys.exit(1)
  }

  private def cloudStorageConfig(config: Config): CloudStorageConfig = {
    val base = CloudStorageConfig.readEnv(None)
    val rpProject = config.gcsRequesterPaysProject
      .orElse(sys.env.get("GCS_REQUESTER_PAYS_PROJECT"))
      .orElse(sys.env.get("GOOGLE_PROJECT"))
      .map(_.trim)
      .filter(_.nonEmpty)
    val rpBuckets = config.gcsRequesterPaysBuckets.orElse(
      sys.env
        .get("GCS_REQUESTER_PAYS_BUCKETS")
        .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSet)
        .filter(_.nonEmpty)
    )

    rpProject match {
      case Some(project) =>
        val rpConfig = Some(RequesterPaysConfig(project, rpBuckets))
        base.copy(google =
          base.google match {
            case Some(gconf) => Some(gconf.copy(requester_pays_config = rpConfig))
            case None => Some(GoogleStorageConfig(None, rpConfig))
          }
        )
      case None =>
        base
    }
  }

  private def withExecuteContext[T](fs: FS)(f: ExecuteContext => T): T = {
    val refs = ReferenceGenome.builtinReferences()
    val tmpdir = sys.env.getOrElse("TMPDIR", "/tmp")
    val timer = new ExecutionTimer("LSeqVdsSparseExtract")
    ExecuteContext.scoped(
      tmpdir = tmpdir,
      localTmpdir = tmpdir,
      backend = LocalBackend,
      references = refs,
      fs = fs,
      timer = timer,
      tempFileManager = new OwningTempFileManager(fs),
      theHailClassLoader = new HailClassLoader(getClass.getClassLoader),
      flags = HailFeatureFlags.fromEnv(),
      irMetadata = new IrMetadata(),
      blockMatrixCache = mutable.Map.empty[String, BlockMatrix],
      compileCache = mutable.Map.empty,
      irCache = mutable.Map.empty[Int, BaseIR],
      coercerCache = mutable.Map.empty[Any, LoweredTableReaderCoercer],
    )(f)
  }

  private def variantDataPath(vds: String): String =
    if (vds.endsWith("/variant_data")) vds else s"${vds.stripSuffix("/")}/variant_data"

  private def printSpec(path: String, spec: AbstractMatrixTableSpec, fs: FS): Unit = {
    val rowsRvd = spec.rowsSpec.rowsSpec
    val entriesRvd = spec.entriesSpec.rowsSpec
    println(s"variant_data: $path")
    println(s"hail version: ${spec.hail_version}")
    println(s"file version: ${spec.file_version}")
    println(s"row key: ${spec.matrix_type.rowKey.mkString(",")}")
    println(s"column key: ${spec.matrix_type.colKey.mkString(",")}")
    println(s"column count: ${spec.colsSpec.partitionCounts.sum}")
    println(s"row partitions: ${spec.rowsSpec.partitionCounts.length}")
    println(s"rows indexed: ${rowsRvd.indexed}")
    println(s"entries indexed: ${entriesRvd.indexed}")
    println(s"row schema: ${spec.matrix_type.rowType.parsableString()}")
    println(s"entry schema: ${spec.matrix_type.entryType.parsableString()}")
    println(s"rows part files: ${rowsRvd.absolutePartPaths(spec.rowsSpec.rowsComponent.absolutePath(path + "/rows")).length}")
    println(s"entries part files: ${entriesRvd.absolutePartPaths(spec.entriesSpec.rowsComponent.absolutePath(path + "/entries")).length}")
    println(s"path exists: ${fs.isDir(path)}")
  }

  private def readVat(fs: FS, path: String, config: Config): VatReadResult = {
    using(Source.fromInputStream(fs.open(path))) { src =>
      val it = src.getLines()
      if (!it.hasNext)
        return VatReadResult(Map.empty, 0L, 0L, 0L)
      val header = it.next().split('\t').zipWithIndex.toMap
      def idx(name: String): Int =
        header.getOrElse(name, fatal(s"VAT is missing required column '$name'"))

      val contigIdx = idx(config.colContig)
      val posIdx = idx(config.colPosition)
      val refIdx = idx(config.colRef)
      val altIdx = idx(config.colAlt)
      val acIdx = idx(config.colAC)
      val anIdx = idx(config.colAN)
      val afIdx = header.get(config.colAF)
      val scIdx = idx(config.colSC)
      val maxRequiredIdx = Seq(contigIdx, posIdx, refIdx, altIdx, acIdx, anIdx, scIdx).max

      println(
        s"VAT columns: contig=${config.colContig}, position=${config.colPosition}, ref=${config.colRef}, alt=${config.colAlt}, " +
          s"ac=${config.colAC}, an=${config.colAN}, af=${config.colAF}, sc=${config.colSC}"
      )

      val retained = mutable.LinkedHashMap.empty[VariantKey, VariantInfo]
      val duplicatedKeys = mutable.HashSet.empty[VariantKey]
      var qualifiedRows = 0L
      var duplicateRows = 0L
      for (line <- it) {
        val fields = line.split("\t", -1)
        if (fields.length > maxRequiredIdx && fields(contigIdx) == config.chrom) {
          val pos = fields(posIdx).toInt
          if (pos >= config.start && pos <= config.end) {
            val ac = parseIntOrZero(fields(acIdx))
            val an = parseIntOrZero(fields(anIdx))
            val af = if (an == 0) 0.0 else ac.toDouble / an.toDouble
            val outputAf = afIdx.map(i => parseDoubleOrZero(fields(i))).getOrElse(af)
            val mac = math.min(ac, math.max(0, an - ac))
            val maf = math.min(af, 1.0 - af)
            if (mac >= config.minMAC && maf <= config.maxMAF) {
              qualifiedRows += 1
              val key = VariantKey(fields(contigIdx), pos, fields(refIdx), fields(altIdx))
              if (!retained.contains(key)) {
                val sc = parseIntOrZero(fields(scIdx))
                val minorAlleleIndex = if (ac == mac) 1 else 0
                retained += key -> VariantInfo(
                  retained.size,
                  key,
                  ac,
                  an,
                  outputAf,
                  mac,
                  maf,
                  sc,
                  minorAlleleIndex,
                )
              } else {
                duplicateRows += 1
                duplicatedKeys += key
              }
            }
          }
        }
      }
      VatReadResult(retained.toMap, qualifiedRows, duplicateRows, duplicatedKeys.size.toLong)
    }
  }

  private def parseIntOrZero(s: String): Int =
    if (s == null || s.isEmpty) 0 else s.toInt

  private def parseDoubleOrZero(s: String): Double =
    if (s == null || s.isEmpty) 0.0 else s.toDouble

  private def readRequestedSampleIds(fs: FS, path: String, sampleCol: Int): Set[String] =
    using(Source.fromInputStream(fs.open(path))) { src =>
      src.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(line => tokenAt(line, sampleCol, path))
        .filter(_.nonEmpty)
        .toSet
    }

  private def tokenAt(line: String, column: Int, path: String): String = {
    val fields = line.split("\\s+")
    if (column >= fields.length)
      fatal(s"line in $path has ${fields.length} columns, cannot read --sample-col $column: $line")
    fields(column)
  }

  private def selectedSampleIndicesFromConfig(
    ctx: ExecuteContext,
    variantPath: String,
    spec: AbstractMatrixTableSpec,
    config: Config,
  ): Option[Array[Int]] =
    config.sampleIndexList match {
      case Some(path) =>
        Some(readSampleIndexList(ctx.fs, path, config.sampleCol, spec.colsSpec.partitionCounts.sum))
      case None =>
        config.sampleList.map(path => readSelectedSampleIndices(ctx, variantPath, spec, path, config.sampleCol))
    }

  private def readSampleIndexList(fs: FS, path: String, sampleCol: Int, sampleCount: Long): Array[Int] = {
    val indices = mutable.LinkedHashSet.empty[Int]
    var requestedRows = 0L
    using(Source.fromInputStream(fs.open(path))) { src =>
      src.getLines().foreach { line =>
        val trimmed = line.trim
        if (trimmed.nonEmpty) {
          requestedRows += 1
          val token = tokenAt(trimmed, sampleCol, path)
          val idx =
            try token.toInt
            catch {
              case _: NumberFormatException =>
                fatal(s"invalid sample index '$token' in $path")
            }
          if (idx < 0 || idx >= sampleCount)
            fatal(s"sample index $idx is outside VDS column range 0..${sampleCount - 1}")
          indices += idx
        }
      }
    }
    if (indices.isEmpty)
      fatal(s"sample index list is empty: $path")
    println(
      s"sample index list requested $requestedRows rows; retained ${indices.size} unique original VDS column indexes"
    )
    indices.toArray
  }

  private def readSelectedSampleIndices(
    ctx: ExecuteContext,
    variantPath: String,
    spec: AbstractMatrixTableSpec,
    sampleListPath: String,
    sampleCol: Int,
  ): Array[Int] = {
    val requested = readRequestedSampleIds(ctx.fs, sampleListPath, sampleCol)
    if (requested.isEmpty)
      fatal(s"sample list is empty: $sampleListPath")

    val colsPath = variantPath + "/cols"
    val colsRvdPath = spec.colsSpec.rowsComponent.absolutePath(colsPath)
    val colPartPaths = spec.colsSpec.rowsSpec.absolutePartPaths(colsRvdPath)
    val colRequestedType = spec.colsSpec.table_type.rowType
    val (colPType0, colDecoderFactory) =
      spec.colsSpec.rowsSpec.typedCodecSpec.buildDecoder(ctx, colRequestedType)
    val colPType = colPType0.asInstanceOf[PStruct]
    val sampleIdIdx = colPType.fieldIdx.getOrElse("s", fatal("column schema has no sample ID field 's'"))
    val sampleIdType = colPType.types(sampleIdIdx).asInstanceOf[PString]

    val matched = mutable.ArrayBuffer.empty[Int]
    val matchedIds = mutable.HashSet.empty[String]
    var sampleIndex = 0
    colPartPaths.foreach { path =>
      ctx.r.pool.scopedRegion { region =>
        using(openNativeRows(ctx.fs, path, colDecoderFactory, ctx.theHailClassLoader, region)) { colStream =>
          var continue = true
          while (continue) {
            region.clear()
            val rowOffset = colStream.nextOffset()
            if (rowOffset == 0L) {
              continue = false
            } else {
              if (colPType.isFieldDefined(rowOffset, sampleIdIdx)) {
                val sampleId = sampleIdType.loadString(colPType.loadField(rowOffset, sampleIdIdx))
                if (requested.contains(sampleId)) {
                  matched += sampleIndex
                  matchedIds += sampleId
                }
              }
              sampleIndex += 1
            }
          }
        }
      }
    }

    val unmatched = requested.size - matchedIds.size
    println(
      s"warning: sample list requested ${requested.size} unique sample IDs; matched ${matchedIds.size}; unmatched $unmatched"
    )
    if (matchedIds.isEmpty)
      fatal("no requested sample IDs matched VDS column sample IDs")
    matched.toArray
  }

  private def makeChunks(start: Int, endInclusive: Int, chunkBp: Int): IndexedSeq[Chunk] = {
    val b = ArraySeq.newBuilder[Chunk]
    val endExclusive = endInclusive + 1
    val firstChunkId = (start - 1) / chunkBp
    var chunkId = firstChunkId
    var chunkStart = chunkId * chunkBp + 1
    while (chunkStart < endExclusive) {
      val chunkEnd = chunkStart + chunkBp
      b += Chunk(chunkId, chunkStart, chunkEnd, chunkStart, chunkEnd)
      chunkId += 1
      chunkStart = chunkId * chunkBp + 1
    }
    b.result()
  }

  private def availableThreads(): Int =
    math.max(1, Runtime.getRuntime.availableProcessors())

  private def chunkOutputPrefix(basePrefix: String, chrom: String, chunk: Chunk): String =
    s"$basePrefix.$chrom.c${chunk.index}"

  private def variantsInInterval(
    retained: Map[VariantKey, VariantInfo],
    start: Int,
    end: Int,
  ): Map[VariantKey, VariantInfo] =
    retained.filter { case (_, info) => info.key.position >= start && info.key.position < end }

  private def addStats(a: ExtractStats, b: ExtractStats): ExtractStats =
    ExtractStats(
      a.rowsRead + b.rowsRead,
      a.rowsInInterval + b.rowsInInterval,
      a.retainedVariantRows + b.retainedVariantRows,
      a.definedEntries + b.definedEntries,
      a.carrierEvents + b.carrierEvents,
    )

  private def scanChunks(
    ctx: ExecuteContext,
    variantPath: String,
    spec: AbstractMatrixTableSpec,
    config: Config,
    retained: Map[VariantKey, VariantInfo],
    selectedSampleIndices: Option[Array[Int]],
    chunks: IndexedSeq[Chunk],
  ): ExtractStats = {
    val threads = config.threads.getOrElse(availableThreads())
    println(s"chunked mode: ${chunks.length} chunks, $threads worker threads")
    val executor = Executors.newFixedThreadPool(threads)
    val basePrefix = config.outPrefix.get
    val noVatFilter = config.vat.isEmpty
    try {
      val futures = chunks.map { chunk =>
        executor.submit(new Callable[ChunkResult] {
          override def call(): ChunkResult = {
            val selected = selectedPartitionRange(ctx, spec, config.chrom, chunk.queryStart, chunk.queryEnd)
            val chunkRetained =
              if (noVatFilter) retained else variantsInInterval(retained, chunk.queryStart, chunk.queryEnd)
            val prefix = chunkOutputPrefix(basePrefix, config.chrom, chunk)
            val stats = scanVariantData(
              ctx,
              variantPath,
              spec,
              config,
              chunkRetained,
              selectedSampleIndices,
              chunk.queryStart,
              chunk.queryEnd,
              chunk.start,
              chunk.end,
              selected,
              Some(prefix),
              0,
              new HailClassLoader(getClass.getClassLoader),
              noVatFilter,
            )
            ChunkResult(chunk, stats)
          }
        })
      }
      val results =
        try futures.map(_.get()).sortBy(_.chunk.index)
        catch {
          case t: Throwable =>
            executor.shutdownNow()
            throw t
        }
      results.foreach { r =>
        val c = r.chunk
        val s = r.stats
        println(
          s"chunk ${c.index} ${c.start}-${c.end - 1}: rows_read=${s.rowsRead}, rows_in_interval=${s.rowsInInterval}, " +
            s"retained_variant_rows=${s.retainedVariantRows}, defined_entry_elements=${s.definedEntries}, carrier_events=${s.carrierEvents}"
        )
      }
      results.foldLeft(ExtractStats(0L, 0L, 0L, 0L, 0L)) { case (acc, r) => addStats(acc, r.stats) }
    } finally {
      executor.shutdown()
    }
  }

  private def scanVariantData(
    ctx: ExecuteContext,
    variantPath: String,
    spec: AbstractMatrixTableSpec,
    config: Config,
    retained: Map[VariantKey, VariantInfo],
    selectedSampleIndices: Option[Array[Int]],
    intervalStart: Int,
    intervalEnd: Int,
    outputStart: Int,
    outputEnd: Int,
    selectedPartitions: Range,
    outPrefix: Option[String],
    limitRows: Int,
    hcl: HailClassLoader,
    noVatFilter: Boolean,
  ): ExtractStats = {
    val rowTablePath = variantPath + "/rows"
    val entryTablePath = variantPath + "/entries"
    val rowRvdPath = spec.rowsSpec.rowsComponent.absolutePath(rowTablePath)
    val entryRvdPath = spec.entriesSpec.rowsComponent.absolutePath(entryTablePath)
    val rowPartPaths = spec.rowsSpec.rowsSpec.absolutePartPaths(rowRvdPath)
    val entryPartPaths = spec.entriesSpec.rowsSpec.absolutePartPaths(entryRvdPath)
    if (rowPartPaths.length != entryPartPaths.length)
      fatal(s"row and entry partition counts differ: ${rowPartPaths.length} vs ${entryPartPaths.length}")
    println(
      s"selected row/entry partitions: ${selectedPartitions.start}..${selectedPartitions.end - 1} (${selectedPartitions.length})"
    )

    val rowRequestedType = spec.rowsSpec.table_type.rowType
    val entryRequestedType = spec.entriesSpec.table_type.rowType
    val (rowPType0, rowDecoderFactory) =
      spec.rowsSpec.rowsSpec.typedCodecSpec.buildDecoder(ctx, rowRequestedType)
    val (entryPType0, entryDecoderFactory) =
      spec.entriesSpec.rowsSpec.typedCodecSpec.buildDecoder(ctx, entryRequestedType)
    val rowPType = rowPType0.asInstanceOf[PStruct]
    val entryRowPType = entryPType0.asInstanceOf[PStruct]
    val locusIdx = rowPType.fieldIdx.getOrElse("locus", fatal("row schema has no locus field"))
    val allelesIdx = rowPType.fieldIdx.getOrElse("alleles", fatal("row schema has no alleles field"))
    val locusType = rowPType.types(locusIdx).asInstanceOf[PLocus]
    val allelesType = rowPType.types(allelesIdx).asInstanceOf[PArray]
    val entriesIdx =
      entryRowPType.fieldIdx.getOrElse(
        MatrixType.entriesIdentifier,
        fatal(s"entry row schema has no '${MatrixType.entriesIdentifier}' field"),
      )
    val entriesType = entryRowPType.types(entriesIdx).asInstanceOf[PArray]
    val entryElementType = entriesType.elementType.asInstanceOf[PStruct]
    val gtField = entryElementType.fieldIdx.get("GT").map("GT" -> _)
      .orElse(entryElementType.fieldIdx.get("LGT").map("LGT" -> _))
    val laField = entryElementType.fieldIdx.get("LA")

    val variantCounts = mutable.HashMap.empty[Int, VariantCounts]
    val carrierUniverseSize = spec.colsSpec.partitionCounts.sum.toInt
    val selectedSampleCount = selectedSampleIndices.map(_.length).getOrElse(carrierUniverseSize)
    val eventOut = outPrefix.map(prefix =>
      new DataOutputStream(new BufferedOutputStream(ctx.fs.create(prefix + ".events.bin")))
    )
    eventOut.foreach { out =>
      writeEventHeader(
        out,
        config.chrom,
        outputStart,
        outputEnd - 1,
        carrierUniverseSize,
        selectedSampleCount,
        config.chunkBp.getOrElse(0),
      )
    }

    var rowsRead = 0L
    var rowsInInterval = 0L
    var retainedVariantRows = 0L
    var definedEntries = 0L
    var carrierEvents = 0L

    try {
      var p = selectedPartitions.start
      var done = false
      RegionPool.scoped { pool =>
        while (p < selectedPartitions.end && !done && (limitRows <= 0 || rowsRead < limitRows)) {
          pool.scopedRegion { region =>
            using(openNativeRows(ctx.fs, rowPartPaths(p), rowDecoderFactory, hcl, region)) {
            rowStream =>
              using(openNativeRows(
                ctx.fs,
                entryPartPaths(p),
                entryDecoderFactory,
                hcl,
                region,
              )) { entryStream =>
                var continue = true
                while (continue && (limitRows <= 0 || rowsRead < limitRows)) {
                  region.clear()
                  val rowOffset = rowStream.nextOffset()
                  val entryOffset = entryStream.nextOffset()
                  if (rowOffset == 0L || entryOffset == 0L) {
                    if (rowOffset != entryOffset)
                      fatal(s"row/entry partition ended at different records in partition $p")
                    continue = false
                  } else {
                    rowsRead += 1
                    val locusOffset = rowPType.loadField(rowOffset, locusIdx)
                    val contig = locusType.contig(locusOffset)
                    val position = locusType.position(locusOffset)
                    if (contig == config.chrom && position >= intervalEnd) {
                      done = true
                      continue = false
                    } else if (contig == config.chrom && position >= intervalStart && position < intervalEnd) {
                      rowsInInterval += 1
                      val allelesOffset = rowPType.loadField(rowOffset, allelesIdx)
                      val alleles = unsafeStringArray(allelesType, allelesOffset)
                      val variantInfoByAllele = alleleVariantInfo(contig, position, alleles, retained)
                      if (noVatFilter || variantInfoByAllele.nonEmpty) {
                        retainedVariantRows += 1
                        val entriesOffset = entryRowPType.loadField(entryOffset, entriesIdx)
                        val sampleCount = entriesType.loadLength(entriesOffset)
                        selectedSampleIndices match {
                          case Some(indices) =>
                            var i = 0
                            while (i < indices.length) {
                              val sampleIndex = indices(i)
                              if (sampleIndex < sampleCount) {
                                val events = processEntry(
                                  entriesType,
                                  entryElementType,
                                  gtField,
                                  laField,
                                  eventOut,
                                  variantCounts,
                                  variantInfoByAllele,
                                  noVatFilter,
                                  entriesOffset,
                                  sampleCount,
                                  sampleIndex,
                                )
                                if (events >= 0) {
                                  definedEntries += 1
                                  carrierEvents += events
                                }
                              }
                              i += 1
                            }
                          case None =>
                            var sampleIndex = 0
                            while (sampleIndex < sampleCount) {
                              val events = processEntry(
                                entriesType,
                                entryElementType,
                                gtField,
                                laField,
                                eventOut,
                                variantCounts,
                                variantInfoByAllele,
                                noVatFilter,
                                entriesOffset,
                                sampleCount,
                                sampleIndex,
                              )
                              if (events >= 0) {
                                definedEntries += 1
                                carrierEvents += events
                              }
                              sampleIndex += 1
                            }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          p += 1
        }
      }
    } finally {
      eventOut.foreach(_.close())
    }

    writeVariants(ctx.fs, outPrefix, retained, variantCounts)

    ExtractStats(rowsRead, rowsInInterval, retainedVariantRows, definedEntries, carrierEvents)
  }

  private def selectedPartitionRange(
    ctx: ExecuteContext,
    spec: AbstractMatrixTableSpec,
    chrom: String,
    start: Int,
    end: Int,
  ): Range = {
    val partitioner = spec.rowsSpec.rowsSpec.partitioner(ctx.stateManager)
    if (partitioner.kType.fieldNames.headOption.forall(_ != "locus"))
      fatal(s"expected row partitioner first key to be locus, found ${partitioner.kType.fieldNames.mkString(",")}")
    val locusPartitioner = partitioner.coarsen(1)
    val query = Interval(
      Row(Locus(chrom, start)),
      Row(Locus(chrom, end - 1)),
      includesStart = true,
      includesEnd = true,
    )
    locusPartitioner.queryInterval(query)
  }

  private def openNativeRows(
    fs: FS,
    path: String,
    decoderFactory: (InputStream, HailClassLoader) => Decoder,
    hcl: HailClassLoader,
    region: Region,
  ): NativeRowStream = {
    val is = fs.openNoCompression(path)
    new NativeRowStream(is, decoderFactory(is, hcl), region)
  }

  private def unsafeStringArray(t: PArray, offset: Long): IndexedSeq[String] = {
    val n = t.loadLength(offset)
    val b = ArraySeq.newBuilder[String]
    b.sizeHint(n)
    var i = 0
    while (i < n) {
      if (t.isElementDefined(offset, i))
        b += UnsafeRow.read(t.elementType, null, t.loadElement(offset, n, i)).asInstanceOf[String]
      else
        b += null
      i += 1
    }
    b.result()
  }

  private def unsafeIntArray(t: PArray, offset: Long): IndexedSeq[Int] = {
    val n = t.loadLength(offset)
    val b = ArraySeq.newBuilder[Int]
    b.sizeHint(n)
    var i = 0
    while (i < n) {
      if (t.isElementDefined(offset, i))
        b += Region.loadInt(t.loadElement(offset, n, i))
      else
        b += -1
      i += 1
    }
    b.result()
  }

  private def localToGlobalAlleles(
    entryElementType: PStruct,
    entryOffset: Long,
    laField: Option[Int],
    call: Int,
  ): IndexedSeq[Int] =
    laField match {
      case Some(laIdx) if entryElementType.isFieldDefined(entryOffset, laIdx) =>
        val laType = entryElementType.types(laIdx).asInstanceOf[PArray]
        val la = unsafeIntArray(laType, entryElementType.loadField(entryOffset, laIdx))
        Call.alleles(call).map { localAllele =>
          if (localAllele >= 0 && localAllele < la.length) la(localAllele) else -1
        }
      case _ =>
        IndexedSeq.empty[Int]
    }

  private def processEntry(
    entriesType: PArray,
    entryElementType: PStruct,
    gtField: Option[(String, Int)],
    laField: Option[Int],
    eventOut: Option[DataOutputStream],
    variantCounts: mutable.Map[Int, VariantCounts],
    variantInfoByAllele: Map[Int, VariantInfo],
    noVatFilter: Boolean,
    entriesOffset: Long,
    sampleCount: Int,
    sampleIndex: Int,
  ): Int = {
    if (!entriesType.isElementDefined(entriesOffset, sampleIndex)) {
      if (!noVatFilter && variantInfoByAllele.contains(0))
        return writeAlleleEvents(
          eventOut,
          variantCounts,
          variantInfoByAllele,
          sampleIndex,
          IndexedSeq(0, 0),
          noVatFilter = false,
        )
      return -1
    }

    val elementOffset = entriesType.loadElement(entriesOffset, sampleCount, sampleIndex)
    gtField match {
      case Some((gtName, gtIdx)) if entryElementType.isFieldDefined(elementOffset, gtIdx) =>
        val call = Region.loadInt(entryElementType.loadField(elementOffset, gtIdx))
        val alleleIndexes =
          if (gtName == "LGT")
            localToGlobalAlleles(entryElementType, elementOffset, laField, call)
          else
            Call.alleles(call)
        if (!noVatFilter || Call.isNonRef(call)) {
          writeAlleleEvents(
            eventOut,
            variantCounts,
            variantInfoByAllele,
            sampleIndex,
            alleleIndexes,
            noVatFilter,
          )
        } else {
          0
        }
      case _ =>
        0
    }
  }

  private def alleleVariantInfo(
    contig: String,
    position: Int,
    alleles: IndexedSeq[String],
    retained: Map[VariantKey, VariantInfo],
  ): Map[Int, VariantInfo] = {
    if (retained.isEmpty || alleles.length < 2)
      Map.empty
    else {
      val ref = alleles(0)
      val b = Map.newBuilder[Int, VariantInfo]
      var i = 1
      while (i < alleles.length) {
        retained.get(VariantKey(contig, position, ref, alleles(i))).foreach { info =>
          if (info.minorAlleleIndex == 0) {
            if (isSnv(ref, alleles(i)))
              b += 0 -> info
          } else {
            b += i -> info
          }
        }
        i += 1
      }
      b.result()
    }
  }

  private def isSnv(ref: String, alt: String): Boolean =
    ref != null && alt != null && ref.length == 1 && alt.length == 1

  private def writeAlleleEvents(
    out: Option[DataOutputStream],
    variantCounts: mutable.Map[Int, VariantCounts],
    variantInfoByAllele: Map[Int, VariantInfo],
    sampleIndex: Int,
    alleleIndexes: IndexedSeq[Int],
    noVatFilter: Boolean,
  ): Int = {
    if (out.isEmpty)
      return if (noVatFilter || variantInfoByAllele.nonEmpty) 1 else 0

    var n = 0
    if (noVatFilter) {
      out.get.writeInt(-1)
      out.get.writeInt(sampleIndex)
      out.get.writeByte(alleleIndexes.count(_ > 0))
      out.get.writeInt(0)
      n = 1
    } else {
      for ((alleleIndex, info) <- variantInfoByAllele) {
        val dosage = alleleIndexes.count(_ == alleleIndex)
        if (dosage > 0) {
          out.get.writeInt(info.index)
          out.get.writeInt(sampleIndex)
          out.get.writeByte(dosage)
          out.get.writeInt(info.sc)
          val counts = variantCounts.getOrElseUpdate(info.index, VariantCounts())
          if (dosage == 1) counts.het += 1
          else if (dosage == 2) counts.hom += 1
          n += 1
        }
      }
    }
    n
  }

  private def writeEventHeader(
    out: DataOutputStream,
    chrom: String,
    start: Int,
    end: Int,
    carrierUniverseSize: Int,
    selectedSampleCount: Int,
    chunkBp: Int,
  ): Unit = {
    val chromBytes = chrom.getBytes(StandardCharsets.UTF_8)
    out.writeInt(0x4c535131) // LSQ1
    out.writeInt(2)
    out.writeInt(carrierUniverseSize)
    out.writeInt(selectedSampleCount)
    out.writeInt(chunkBp)
    out.writeInt(start)
    out.writeInt(end)
    out.writeInt(chromBytes.length)
    out.write(chromBytes)
  }

  private def writeVariants(
    fs: FS,
    outPrefix: Option[String],
    retained: Map[VariantKey, VariantInfo],
    variantCounts: collection.Map[Int, VariantCounts],
  ): Unit =
    outPrefix.foreach { prefix =>
      using(fs.create(prefix + ".variants.tsv")) { os =>
        val header = "variant_index\tcontig\tposition\tref\talt\tminor_allele\tac\tan\taf\tmac\tmaf\tsc\n"
        os.write(header.getBytes("UTF-8"))
        retained.values.toIndexedSeq.sortBy(_.index).foreach { info =>
          variantCounts.get(info.index).foreach { counts =>
            val eventMac = counts.het + 2 * counts.hom
            if (eventMac > 0) {
              val k = info.key
              val eventMaf = if (info.an == 0) 0.0 else eventMac.toDouble / info.an.toDouble
              val minorAllele = if (info.minorAlleleIndex == 0) "ref" else "alt"
              val line =
                s"${info.index}\t${k.contig}\t${k.position}\t${k.ref}\t${k.alt}\t$minorAllele\t${info.ac}\t${info.an}\t${info.af}\t$eventMac\t$eventMaf\t${info.sc}\n"
              os.write(line.getBytes("UTF-8"))
            }
          }
        }
      }
    }
}
