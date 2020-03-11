from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col, struct


# source directories
all_srcdir = 's3://dig-analysis-data/out/varianteffect/variants/part-*'
freq_srcdir = 's3://dig-analysis-data/out/frequencyanalysis/'
tf_srcdir = 's3://dig-analysis-data/out/transcriptionfactors/part-*'
# vep_srcdir = 's3://dig-analysis-data/out/varianteffect/transcript_consequences/part-*'
vep_srcdir = 's3://dig-analysis-data/out/varianteffect/effects/part-*'

# output directory
outdir = 's3://dig-bio-index/variants'

# variant list schema
all_schema = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('allele', StringType(), nullable=False),
        StructField('strand', StringType(), nullable=False),
        StructField('varId', StringType(), nullable=False),
    ]
)

# this is the schema written out by the frequency analysis processor
frequency_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('eaf', DoubleType(), nullable=False),
        StructField('maf', DoubleType(), nullable=False),
        StructField('ancestry', StringType(), nullable=False),
    ]
)

# transcription factor schema
tf_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('positionWeightMatrix', StringType(), nullable=False),
        StructField('delta', DoubleType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('strand', StringType(), nullable=False),
        StructField('refScore', DoubleType(), nullable=False),
        StructField('altScore', DoubleType(), nullable=False),
    ]
)

# VEP schema
vep_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('aminoAcids', StringType(), nullable=True),
        StructField('biotype', StringType(), nullable=True),
        StructField('caddPhred', DoubleType(), nullable=True),
        StructField('caddRaw', DoubleType(), nullable=True),
        StructField('caddRawRankscore', DoubleType(), nullable=True),
        StructField('canonical', BooleanType(), nullable=True),
        StructField('ccds', StringType(), nullable=True),
        StructField('cdnaEnd', IntegerType(), nullable=True),
        StructField('cdnaStart', IntegerType(), nullable=True),
        StructField('cdsEnd', IntegerType(), nullable=True),
        StructField('cdsStart', IntegerType(), nullable=True),
        StructField('clinvarClnsig', StringType(), nullable=True),
        StructField('clinvarGoldenStars', StringType(), nullable=True),
        StructField('clinvarRs', StringType(), nullable=True),
        StructField('clinvarTrait', StringType(), nullable=True),
        StructField('codons', StringType(), nullable=True),
        StructField('consequenceTerms', StringType(), nullable=True),
        StructField('dannRankscore', DoubleType(), nullable=True),
        StructField('dannScore', DoubleType(), nullable=True),
        StructField('distance', IntegerType(), nullable=True),
        StructField('eigenPcRaw', DoubleType(), nullable=True),
        StructField('eigenPcRawRankscore', DoubleType(), nullable=True),
        StructField('eigenPhred', DoubleType(), nullable=True),
        StructField('eigenRaw', DoubleType(), nullable=True),
        StructField('fathmmConvertedRankscore', DoubleType(), nullable=True),
        StructField('fathmmMklCodingPred', StringType(), nullable=True),
        StructField('fathmmMklCodingRankscore', DoubleType(), nullable=True),
        StructField('fathmmMklCodingScore', DoubleType(), nullable=True),
        StructField('fathmm_converted_rankscore', StringType(), nullable=True),
        StructField('fathmmPred', StringType(), nullable=True),
        StructField('fathmmScore', StringType(), nullable=True),
        StructField('flags', StringType(), nullable=True),
        StructField('geneId', StringType(), nullable=True),
        StructField('genocanyonScore', DoubleType(), nullable=True),
        StructField('genocanyonScoreRankscore', DoubleType(), nullable=True),
        StructField('gerpPlusPlusNr', DoubleType(), nullable=True),
        StructField('gerpPlusPlusRs', DoubleType(), nullable=True),
        StructField('gerpPlusPlusRsRankscore', DoubleType(), nullable=True),
        StructField('gm12878ConfidenceValue', DoubleType(), nullable=True),
        StructField('gm12878FitconsScore', DoubleType(), nullable=True),
        StructField('gm12878FitconsScoreRankscore', DoubleType(), nullable=True),
        StructField('gtexV6pGene', StringType(), nullable=True),
        StructField('gtexV6pTissue', StringType(), nullable=True),
        StructField('h1HescConfidenceValue', DoubleType(), nullable=True),
        StructField('h1HescFitconsScore', DoubleType(), nullable=True),
        StructField('h1HescFitconsScoreRankscore', DoubleType(), nullable=True),
        StructField('huvecConfidenceValue', DoubleType(), nullable=True),
        StructField('huvecFitconsScore', DoubleType(), nullable=True),
        StructField('huvecFitconsScoreRankscore', DoubleType(), nullable=True),
        StructField('impact', StringType(), nullable=True),
        StructField('integratedConfidenceValue', DoubleType(), nullable=True),
        StructField('integratedFitconsScore', DoubleType(), nullable=True),
        StructField('integratedFitconsScoreRankscore', DoubleType(), nullable=True),
        StructField('interpro_domain', StringType(), nullable=True),
        StructField('lof', StringType(), nullable=True),
        StructField('lofFilter', StringType(), nullable=True),
        StructField('lofFlags', StringType(), nullable=True),
        StructField('lofInfo', StringType(), nullable=True),
        StructField('lrtConvertedRankscore', DoubleType(), nullable=True),
        StructField('lrtOmega', DoubleType(), nullable=True),
        StructField('lrtPred', StringType(), nullable=True),
        StructField('lrtScore', DoubleType(), nullable=True),
        StructField('metalrPred', StringType(), nullable=True),
        StructField('metalrRankscore', DoubleType(), nullable=True),
        StructField('metalrScore', DoubleType(), nullable=True),
        StructField('metasvmPred', StringType(), nullable=True),
        StructField('metasvmRankscore', DoubleType(), nullable=True),
        StructField('metasvmScore', DoubleType(), nullable=True),
        StructField('mutationAssessorPred', StringType(), nullable=True),
        StructField('mutationAssessorScore', DoubleType(), nullable=True),
        StructField('mutationAssessorScoreRankscore', DoubleType(), nullable=True),
        StructField('mutationAssessorUniprotId', StringType(), nullable=True),
        StructField('mutationAssessorVariant', StringType(), nullable=True),
        StructField('mutationTasterAae', StringType(), nullable=True),
        StructField('mutationTasterConvertedRankscore', DoubleType(), nullable=True),
        StructField('mutationTasterModel', StringType(), nullable=True),
        StructField('mutationTasterPred', StringType(), nullable=True),
        StructField('mutationTasterScore', StringType(), nullable=True),
        StructField('phastcons100WayVertebrate', DoubleType(), nullable=True),
        StructField('phastcons100WayVertebrateRankscore', DoubleType(), nullable=True),
        StructField('phastcons20WayMammalian', DoubleType(), nullable=True),
        StructField('phastcons20WayMammalianRankscore', DoubleType(), nullable=True),
        StructField('phylop100WayVertebrate', DoubleType(), nullable=True),
        StructField('phylop100WayVertebrateRankscore', DoubleType(), nullable=True),
        StructField('phylop20WayMammalian', DoubleType(), nullable=True),
        StructField('phylop20WayMammalianRankscore', DoubleType(), nullable=True),
        StructField('pick', IntegerType(), nullable=True),
        StructField('polyphen2HdivPred', StringType(), nullable=True),
        StructField('polyphen2HdivRankscore', DoubleType(), nullable=True),
        StructField('polyphen2HdivScore', DoubleType(), nullable=True),
        StructField('polyphen2HvarPred', StringType(), nullable=True),
        StructField('polyphen2HvarRankscore', DoubleType(), nullable=True),
        StructField('polyphen2HvarScore', DoubleType(), nullable=True),
        StructField('polyphenPrediction', StringType(), nullable=True),
        StructField('polyphenScore', DoubleType(), nullable=True),
        StructField('proteinEnd', IntegerType(), nullable=True),
        StructField('proteinStart', IntegerType(), nullable=True),
        StructField('proveanConvertedRankscore', DoubleType(), nullable=True),
        StructField('proveanPred', StringType(), nullable=True),
        StructField('proveanScore', DoubleType(), nullable=True),
        StructField('reliabilityIndex', IntegerType(), nullable=True),
        StructField('siftConvertedRankscore', DoubleType(), nullable=True),
        StructField('siftPred', StringType(), nullable=True),
        StructField('siftPrediction', StringType(), nullable=True),
        StructField('siftScore', DoubleType(), nullable=True),
        StructField('siphy29WayLogodds', DoubleType(), nullable=True),
        StructField('siphy29WayLogoddsRankscore', DoubleType(), nullable=True),
        StructField('siphy29WayPi', StringType(), nullable=True),
        StructField('strand', IntegerType(), nullable=True),
        StructField('transcriptId', StringType(), nullable=True),
        StructField('transcriptIdVest3', StringType(), nullable=True),
        StructField('transcriptVarVest3', StringType(), nullable=True),
        StructField('variantAllele', StringType(), nullable=True),
        StructField('vest3Rankscore', DoubleType(), nullable=True),
        StructField('vest3Score', DoubleType(), nullable=True),
    ]
)


def load_freq(ancestry_name):
    return spark.read \
        .csv('%s/%s/part-*' % (freq_srcdir, ancestry_name), sep='\t', header=True, schema=frequency_schema) \
        .select(col('varId'), struct('eaf', 'maf').alias(ancestry_name))


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the unique variants
    variants = spark.read.csv(all_srcdir, sep='\t', header=False, schema=all_schema) \
        .select('varId', 'chromosome', 'position')

    # frequency outputs by ancestry
    ancestries = ['AA', 'AF', 'EA', 'EU', 'HS', 'SA']
    freq = None

    # load frequencies by variant ID
    for ancestry in ancestries:
        df = load_freq(ancestry)

        # final, joined frequencies
        freq = df if freq is None else freq.join(df, 'varId', how='outer')

    # pull all the frequencies together into a single map
    freq = freq.select(freq.varId, struct(*ancestries).alias('frequency'))

    # load transcription factors and group them by varId
    tfs = spark.read.csv(tf_srcdir, sep='\t', header=True, schema=tf_schema) \
        .rdd \
        .keyBy(lambda r: r.varId) \
        .combineByKey(lambda r: [r], lambda c, r: c + [r], lambda c, rs: c + rs) \
        .toDF() \
        .select(
            col('_1').alias('varId'),
            col('_2').alias('transcriptionFactors'),
        )

    # load transcript consequences
    # cqs = spark.read.csv(vep_srcdir, sep='\t', header=True, schema=vep_schema) \
    #    .select('varId', struct('*').alias('transcriptConsequence'))
    vep = spark.read.json(vep_srcdir) \
        .select(
            col('id').alias('varId'),
            struct('*').alias('effects'),
        )

    # join everything together, remove empty records
    df = variants \
        .join(freq, 'varId', how='left_outer') \
        .join(tfs, 'varId', how='left_outer') \
        .join(vep, 'varId', how='left_outer') \
        .filter(col('varId').isNotNull()) \

    # write out variant data by ID
    df.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()
