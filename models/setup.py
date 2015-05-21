__author__ = 'dex'

import re, os, gzip, subprocess, sys
from cStringIO import StringIO


def uni_cat(fields, fieldSep="|\n|"):
    """Unique concatenation of """
    return fieldSep.join(fields.astype(str).unique())


def walk_files(dir, endswith=".gz"):
    # traverse root directory, and list directories as dirs and files as files
    for root, dirs, filenames in os.walk(dir):
        for filename in filenames:
            if filename.endswith(endswith): yield os.path.join(root, filename)


def get_clean_columns(columns):
    p = re.compile('[\W_]+')
    clean = [p.sub('_', col).lower() for col in columns]
    return clean


def get_df_from_stream(stream, entity):
    import pandas as pd

    stream.seek(0)
    index_col = '%s_title' % entity
    # open("sample.txt", "wb").write(stream.getvalue())
    df = pd.io.parsers.read_table(stream) \
        .dropna(how='all') \
        .groupby(index_col) \
        .aggregate(uni_cat) \
        .T
    df.index.name = index_col
    df = df.reset_index()  # may want to store title
    df.columns = get_clean_columns(df.columns)
    return df


def create_indices_on_postgres(indices, unique=True):
    # Modified from http://osdir.com/ml/web2py/2010-09/msg00952.html
    """Creates a set of indices if they do not exist"""
    # # Edit this list of table columns to index
    # # The format is [('table', 'column1, column2, ...'),...]
    print "checking indicies:"
    for table, columns in indices:
        relname = "%s_%s_idx" % (table.replace("_", ""),
                                 columns.replace(" ", "") \
                                 .replace("_", "") \
                                 .replace(",", "_"))
        print "\t%s" % relname,
        index_exists = \
            db.executesql("select count(*) from pg_class where relname = '%s';" % relname)[0][0] == 1
        if not index_exists:
            print "CREATING"
            sql = 'create' \
                  + (' unique' if unique else '') \
                  + ' index %s on %s (%s);' % (relname, table, columns)
            print sql
            db.executesql(sql)
            db.commit()
        print "OK"

    print "Done!"


def get_entity2stream(filename, entities):
    entity2stream = {}
    try:
        for line in gzip.open(filename, 'rb'):
            for entity in entities:
                if entity not in entity2stream:
                    entity2stream[entity] = StringIO()
                header = "!%s_" % entity
                if line.startswith(header):
                    print >> entity2stream[entity], line[len("!"):]
    except IOError:
        f = open("errors.txt", "a")
        print >> f, "IOError in %s" % filename
        f.close()

    return entity2stream


def get_sample_cross_tab():
    print "creating header view"
    create_sample_attribute_header_view()
    headers = db(Sample_Attribute_Header).select(Sample_Attribute_Header.header)
    print "creating view"
    # INDEX source attribute
    create_indices_on_postgres([('sample_attribute', 'attribute_name')], unique=False)  # for view cols
    create_indices_on_postgres([('sample_attribute', 'sample_id, attribute_name')])  # for aggregate function

    # CREATE VIEW
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS sample_view CASCADE;")
    db.executesql("DROP INDEX IF EXISTS sample_view_fts_idx CASCADE;")
    db.executesql("DROP SEQUENCE IF EXISTS sample_attribute_sequence CASCADE;")
    db.executesql("""CREATE SEQUENCE sample_attribute_sequence;""")
    attributesSql = "," \
        .join(["""string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///') as %s \n""" \
               % (row.header, row.header)
               for row in headers])
    docSql = "to_tsvector('english', gse_name) || " + \
             "||".join([
                 """to_tsvector('english', coalesce(string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///'), ''))\n""" \
                 % (row.header)
                 for row in headers]) + " AS doc"

    sql = """CREATE MATERIALIZED VIEW sample_view AS
             SELECT NEXTVAL('sample_attribute_sequence') as id,
             %s,
             series_id, platform_id, sample_id,
             %s
             FROM sample
                 JOIN sample_attribute ON sample.id = sample_id
                 JOIN series ON series.id = series_id
                 JOIN platform ON platform.id = platform_id
               GROUP BY gse_name, series_id, platform_id, sample_id;""" % (docSql, attributesSql)
    print sql
    db.executesql(sql)
    create_indices_on_postgres([('sample_view', 'id')])
    create_indices_on_postgres([('sample_view', 'series_id')], unique=False)
    print "indexing FTS"
    sql = "CREATE INDEX sample_view_fts_idx ON sample_view USING gin(doc);"
    # print sql
    db.executesql(sql)

    # reset results
    Sample_View_Results.truncate()
    db.commit()


def get_series_cross_tab():
    print "creating header view"
    create_series_attribute_header_view()
    headers = db(Series_Attribute_Header).select(Series_Attribute_Header.header)
    print "creating view"
    # INDEX source attribute
    create_indices_on_postgres([('series_attribute', 'attribute_name')], unique=False)  # for view cols
    create_indices_on_postgres([('series_attribute', 'series_id, attribute_name')])  # for aggregate
    # CREATE VIEW
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS series_view CASCADE ;")
    db.executesql("DROP INDEX IF EXISTS series_view_fts_idx CASCADE ;")
    db.executesql("DROP SEQUENCE IF EXISTS series_attribute_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE series_attribute_sequence;")
    attributesSql = "," \
        .join(["""string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///') as %s \n""" \
               % (row.header, row.header)
               for row in headers])
    docSql = "||" \
                 .join([
        """to_tsvector('english', coalesce(string_agg(CASE attribute_name WHEN '%s' THEN attribute_value END, '///'), ''))\n""" \
        % (row.header)
        for row in headers]) + " AS doc"

    sql = """CREATE MATERIALIZED VIEW series_view AS
             SELECT NEXTVAL('series_attribute_sequence') as id,
             %s,
             series_id,
             %s
             FROM series_attribute
             GROUP BY series_id;""" % (docSql, attributesSql)
    print sql
    db.executesql(sql)
    create_indices_on_postgres([('series_view', 'id')])
    print "indexing FTS"
    sql = "CREATE INDEX series_view_fts_idx ON series_view USING gin(doc);"
    # print sql
    db.executesql(sql)
    # reset results
    Series_View_Results.truncate()
    # rebuild headers
    db.commit()

def get_sample_tag_cross_tab():
    print "reading tag names"
    rows = db(Tag).select(orderby=Tag.tag_name)

    # CREATE VIEW
    print "creating view"
    db.executesql("DROP SEQUENCE IF EXISTS sample_tag_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE sample_tag_sequence;")
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS sample_tag_view ;")

    create_indices_on_postgres([('sample_tag', 'sample_id, series_tag_id')])  # for aggregate function

    # print "creating view"
    tagsSql = "," \
        .join(["""string_agg(CASE tag_id WHEN %s THEN annotation END, '|||') as %s \n""" \
               % (row['id'], row['tag_name'])
               for row in rows])

    docSql = """to_tsvector('english', gse_name) ||
                to_tsvector('english', gpl_name) ||
                to_tsvector('english', gsm_name) || """ + \
             "||".join([
                 """
                 to_tsvector('english', coalesce(string_agg(CASE tag_name WHEN '%s' THEN annotation || ' ' || tag_name ||  ' ' || description END, '///'), ''))
                 """ % (row['tag_name'])
                 for row in rows]) + " AS doc"
    #
    sql = """CREATE MATERIALIZED VIEW sample_tag_view AS
             SELECT NEXTVAL('sample_tag_sequence') as id, \
            %s,
             series.id as series_id,
             platform.id as platform_id,
             sample.id as sample_id,
             %s

             FROM sample_tag
                 JOIN series_tag ON sample_tag.series_tag_id = series_tag.id
                 JOIN tag ON tag.id = tag_id
                 JOIN series ON series.id = series_tag.series_id
                 JOIN platform ON platform.id = platform_id
                 JOIN sample ON sample.id = sample_id
               GROUP BY gse_name, gpl_name, gsm_name, series.id, platform.id, sample.id;""" % (docSql, tagsSql)
    print sql
    db.executesql(sql)
    print "indexing FTS"
    sql = "CREATE INDEX sample_tag_view_fts_idx ON sample_tag_view USING gin(doc);"
    # print sql
    db.executesql(sql)

    get_series_tag_cross_tab()

    # reset results
    Sample_Tag_View_Results.truncate()
    db.commit()

    #update  homepage counts
    update_counts()
    db.commit()


def get_series_tag_cross_tab():
    print "reading tag names"
    rows = db().select(Tag.tag_name,
                       orderby=Tag.tag_name,
                       distinct=True)
    # CREATE VIEW
    print "creating view"
    db.executesql("DROP SEQUENCE IF EXISTS series_tag_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE series_tag_sequence;")
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS series_tag_view ;")

    db.executesql(
        """
        CREATE OR REPLACE FUNCTION concat_tsvectors(tsv1 TSVECTOR, tsv2 TSVECTOR)
          RETURNS TSVECTOR AS $$
        BEGIN
          RETURN coalesce(tsv1, to_tsvector('english', ''))
                 || coalesce(tsv2, to_tsvector('english', ''));
        END;
        $$ LANGUAGE plpgsql;
        """)

    db.executesql("DROP AGGREGATE IF EXISTS tsvector_agg( TSVECTOR );")

    db.executesql("""
            CREATE AGGREGATE tsvector_agg (
            BASETYPE = TSVECTOR,
            SFUNC = concat_tsvectors,
            STYPE = TSVECTOR,
            INITCOND = ''
            );"""
    )
    tagsSql = "," \
        .join(["""count(%s) as %s \n""" \
               % (row['tag_name'], row['tag_name'])
               for row in rows])
    sql = """CREATE MATERIALIZED VIEW series_tag_view AS
             SELECT NEXTVAL('series_tag_sequence') as id,
             series_id, platform_id,
             tsvector_agg(doc) as doc,
             count(*) as samples,""" \
          + tagsSql \
          + """ FROM sample_tag_view
               GROUP BY series_id, platform_id;"""
    print sql
    db.executesql(sql)
    print "indexing FTS"
    sql = "CREATE INDEX series_tag_view_fts_idx ON series_tag_view USING gin(doc);"
    # print sql
    # reset results
    Series_Tag_View_Results.truncate()

    db.executesql(sql)


def insert_attributes():
    isLast = lastGse = False
    row = db.executesql("select gse_name from series order by id desc limit 1;")
    if row:
        lastGse = row[0][0]
    # for filename in walk_files("/Users/dex/geo_mirror/DATA/SeriesMatrix"):
    for filename in walk_files("/Volumes/Archives/geo_mirror/DATA/SeriesMatrix"):
        basename = os.path.basename(filename)
        gse_name = basename.split("_")[0].split("-")[0]
        print basename
        # if (gse_name == lastGse) or not lastGse:
        # isLast = True
        # if not isLast:
        # print "\tskipping"
        # continue

        entities = "Series Platform Sample".split()
        entity2stream = get_entity2stream(filename, entities)

        # print "\tbuilding series"
        series = get_df_from_stream(entity2stream['Series'], 'Series')
        # series.to_csv('series.csv')
        if ('series_type' not in series.columns) or (series.series_type != "Expression profiling by array").any():
            # print "skipping for", ('type' in series.columns) and series.type
            continue
        if ('series_platform_taxid' not in series.columns) or (series.series_platform_taxid != '9606').any():
            # print "skipping for", ('platform_taxid' in series.columns) and series.platform_taxid
            continue
        assert len(series.index) == 1
        series_rec = db(Series.gse_name == gse_name).select().first() \
                     or Series(Series.insert(gse_name=gse_name))
        for attribute_name in series.columns:
            attribute_value = uni_cat(series[attribute_name])
            # attribute_name = attribute_name.replace("Series_", "")
            series_attribute_rec = db((Series_Attribute.series_id == series_rec.id) & \
                                      (Series_Attribute.attribute_name == attribute_name)).select().first() \
                                   or Series_Attribute(Series_Attribute.insert(series_id=series_rec.id,
                                                                               attribute_name=attribute_name,
                                                                               attribute_value=attribute_value))

        # print "\tbuilding samples",
        samples = get_df_from_stream(entity2stream['Sample'], 'Sample')
        gpls = samples['sample_platform_id'].unique()
        assert len(gpls) == 1
        gpl_name = gpls[0]
        print "\t", gpl_name
        platform_rec = db(Platform.gpl_name == gpl_name).select().first() \
                       or Platform(Platform.insert(gpl_name=gpl_name))

        samples['gsm_name'] = samples.sample_geo_accession
        samples = samples.set_index('gsm_name')
        for gsm_name in samples.index:
            sample_rec = db((Sample.gsm_name == gsm_name) & \
                            (Sample.series_id == series_rec.id) & \
                            (Sample.platform_id == platform_rec.id)).select().first() \
                         or Sample(Sample.insert(gsm_name=gsm_name,
                                                 series_id=series_rec.id,
                                                 platform_id=platform_rec.id))
            attribute_name2value = samples.ix[gsm_name].to_dict()
            for attribute_name in attribute_name2value:
                attribute_value = attribute_name2value[attribute_name].strip()
                # if attribute_value:
                # if type(attribute_value) == str:
                if attribute_value:
                    attribute_value = attribute_value.decode('utf8').encode('utf8')  # weir UTF problem with GSE31631
                    # attribute_name = attribute_name.replace("Sample_", "")
                    sample_attribute_rec = db((Sample_Attribute.sample_id == sample_rec.id) & \
                                              (Sample_Attribute.attribute_name == attribute_name)).select().first() \
                                           or Sample_Attribute(Sample_Attribute.insert(sample_id=sample_rec.id,
                                                                                       attribute_name=attribute_name,
                                                                                       attribute_value=attribute_value))
        db.commit()


def getFirstMatch(queryGroup, query2mapping):
    for query in queryGroup:
        if query in query2mapping:
            return query2mapping[query]


def splitFields(S, toExclude=None):
    """Strips out all punctuation except toExclude"""
    from string import translate, maketrans, punctuation

    if toExclude:
        punctuation = punctuation.replace(toExclude, "")
    S = str(S)
    T = maketrans(punctuation, ' ' * len(punctuation))
    return translate(S, T).split()


def get_dna_probes(gpl, probes, identifier):
    import itertools, glob
    from Bio import SearchIO

    probesFilename = "%s.probes" % gpl

    fasta = ">" + probes.index.map(str) + "\n" + probes[identifier]
    open(probesFilename + ".fa", "w").write("\n".join(fasta))
    # for line in fasta:
    # print line
    faFilename = probesFilename + ".fa"
    pslFilename = probesFilename + "_" + "refMrna.psl"
    if not glob.glob(pslFilename):
        cmd = """/Users/dex/bin/blat
                 /Users/dex/Copy/refMrna.fa
                 -stepSize=5
                 -repMatch=2253
                 -minScore=0
                 -minIdentity=0
                 %s %s""" % (faFilename, pslFilename)
        print "BLATTING RefSeq mRNAs..."
        output = subprocess.call(cmd.split(), stdout=sys.stdout, stderr=sys.stderr)
        print "done"
    print "parsing", pslFilename
    query2hsp = {}

    parser = SearchIO.parse(pslFilename, "blat-psl")
    for result in parser:
        score, hsp = \
            sorted(itertools.chain.from_iterable(
                [[(hsp.score, hsp) for hsp in hit] for hit in result]),
                   reverse=True)[0]
        query2hsp[hsp.query_id] = hsp.hit_id
    probes['refMrna'] = probes.index.map(query2hsp.get)
    return probes


def get_myGene_probes(gpl_name, probes, identifier, scopes=None):
    """Queries mygene.info for current entrezid and sym, given an identifier."""
    import pandas as pd, mygene, itertools

    if scopes == "dna":
        probes = get_dna_probes(gpl_name, probes, identifier)
        identifier = "refMrna"
        scopes = "accession"

    mg = mygene.MyGeneInfo()
    ids = probes[identifier]
    toExclude = None

    if scopes == "unigene":
        toExclude = "."
    if scopes == "accession":
        toExclude = "_"
    idGroups = ids.map(lambda x: splitFields(x, toExclude))

    # http://stackoverflow.com/questions/15321138/removing-unicode-u2026-like-characters-in-a-string-in-python2-7
    allIds = [id.decode('unicode_escape').encode('ascii', 'ignore')  # clean unicode for mygene
              for id in
              set(itertools.chain.from_iterable(idGroups))]

    result = None
    while allIds and not result:
        try:
            result = mg.querymany(allIds,
                                  scopes=scopes,
                                  species='human',
                                  email="dexter@stanford.edu")
        except AssertionError as e:
            print "Error:", e
            print "Retrying"
    df = pd.DataFrame(result)
    probes['myGene_entrez'] = None
    probes['myGene_sym'] = None
    if 'entrezgene' in df.columns:
        query2entrezgene = dict(df[['query', 'entrezgene']].dropna().itertuples(index=False))
        query2symbol = dict(df[['query', 'symbol']].dropna().itertuples(index=False))
        probes['myGene_entrez'] = idGroups.map(lambda queryGroup: getFirstMatch(queryGroup, query2entrezgene))
        probes['myGene_sym'] = idGroups.map(lambda queryGroup: getFirstMatch(queryGroup, query2symbol))
    return probes


def filter_stream(inStream, beginToken="!platform_table_begin", endToken="!platform_table_end", head=None):
    """filters out lines of a file stream that are delimited by 2 tokens, a start and stop.
    Tokens have to match the beginning of a line."""
    import cStringIO

    outStream = cStringIO.StringIO()
    inSlice = False if beginToken else True
    count = 0
    for line in inStream:
        if inSlice:
            if line.startswith(endToken) \
                    or (head is not None) and (count > head):
                break
            count += 1
            outStream.write(line)
        elif line.startswith(beginToken):
            inSlice = True
            continue
    if outStream.tell():
        outStream.seek(0)
        return outStream


def get_probes_from_files(gpl_filenames):
    import pandas as pd

    gpl_filename = gpl_filenames[0]
    print "Reading SOFT from", gpl_filename
    # set up probes
    inStream = gzip.open(gpl_filename)
    stream = filter_stream(inStream)
    df = pd.io.parsers.read_table(stream, index_col=0)
    df.index = df.index.map(str)
    for supplementary_filename in gpl_filenames[1:]:
        print supplementary_filename
        stream = gzip.open(supplementary_filename)
        try:
            supp_df = pd.io.parsers.read_table(stream, index_col=0)
        except pd.parser.CParserError as e:
            print e,
            continue
        supp_df.index = supp_df.index.map(str)
        # drop duplicate ids to deal with exon probes like GPL10585
        supp_df = supp_df.reset_index()
        index_col = supp_df.columns[0]
        supp_df = supp_df.drop_duplicates(subset=[index_col]).set_index(index_col)

        df = pd.concat([df, supp_df])
    print "Done %s probes" % len(df.index)

    return df


def get_probe_info(gpl_name):
    import pandas as pd, glob
    # faster to read annotation if available
    gpl_filename = "/Volumes/Archives/geo_mirror/DATA/annotation/platforms/%s.annot.gz" % gpl_name

    if not glob.glob(gpl_filename):
        # slower to load full family gene
        gpl_filename = "/Volumes/Archives/geo_mirror/DATA/SOFT/by_platform/%s/%s_family.soft.gz" % (gpl_name, gpl_name)

    gpl_filenames = [gpl_filename] + [filename for filename in
                                      glob.glob(
                                          "/Volumes/Archives/geo_mirror/DATA/supplementary/platforms/%s/*.txt.gz" % gpl_name)
                                      if '.cdf.' not in filename.lower()]

    print "Reading SOFT from", gpl_filename
    # set up probes
    inStream = gzip.open(gpl_filename)
    stream = filter_stream(inStream, head=0)
    columns = []
    if stream:
        df = pd.io.parsers.read_table(stream, index_col=0)
        df.index = df.index.map(str)
        for supplementary_filename in gpl_filenames[1:]:
            inStream = gzip.open(supplementary_filename)
            stream = filter_stream(inStream, beginToken=None, head=0)
            supp_df = pd.io.parsers.read_table(stream, index_col=0).head(0)
            supp_df.index = supp_df.index.map(str)
            df = pd.concat([df, supp_df])
        columns = df.columns

    columns = [col \
                   .lower() \
                   .replace("_", "") \
                   .replace(" ", "") for col in columns]

    identifier_scopes = get_identifier_scopes(columns)
    return gpl_filenames, columns, identifier_scopes


def get_identifier_scopes(columns):
    identifier_scopes = []

    # DNA FIRST
    scopes = "dna"
    if "sequence" in columns:
        identifier_scopes.append(("sequence", scopes))
    if "platformsequence" in columns:
        identifier_scopes.append(("platformsequence", scopes))
    if "probesequence" in columns:
        identifier_scopes.append(("probesequence", scopes))


    # ENTREZ
    scopes = "entrezgene, retired"
    if "entrez" in columns:
        identifier_scopes.append(("entrez", scopes))
    if "entrezid" in columns:
        identifier_scopes.append(("entrezid", scopes))
    if "entrezgene" in columns:
        identifier_scopes.append(("entrezgene", scopes))
    if "entrezgeneid" in columns:
        identifier_scopes.append(("entrezgeneid", scopes))
    if "locus" in columns:
        identifier = "locus"
    if "gene" in columns:
        identifier = "gene"
    if "compositesequencebiosequencedatabaseentry[geneid(locusid)]" in columns:
        identifier = "compositesequencebiosequencedatabaseentry[geneid(locusid)]"

    # Ensembl
    scopes = "ensemblgene"
    if "ensembl" in columns:
        identifier_scopes.append(("ensembl", scopes))
    if "ensemblid" in columns:
        identifier_scopes.append(("ensemblid", scopes))
    if "ensemblgene" in columns:
        identifier_scopes.append(("ensemblgene", scopes))
    if "ensemblgeneid" in columns:
        identifier_scopes.append(("ensemblgeneid", scopes))
    if "ensgid" in columns:
        identifier_scopes.append(("ensgid", scopes))

    # Ambiguous geneid
    scopes = "entrezgene, retired, ensemblgene"
    if "geneid" in columns:
        identifier_scopes.append(("geneid", scopes))
    if "compositesequencebiosequencedatabaseentry[geneid]" in columns:
        identifier_scopes.append(("compositesequencebiosequencedatabaseentry[geneid]", scopes))


    # Ambiguous ORFs
    scopes = "entrezgene, retired, ensemblgene, symbol"
    if "orf" in columns:
        identifier_scopes.append(("orf", scopes))

    # Unigenes
    scopes = "unigene"
    if "compositesequencebiosequencedatabaseentry[clusterid]" in columns:
        identifier_scopes.append(("compositesequencebiosequencedatabaseentry[clusterid]", scopes))

    # GB ACCESSIONs
    scopes = "accession"
    if "gbacc" in columns:
        identifier_scopes.append(("gbacc", scopes))
    if "genebankacc" in columns:
        identifier_scopes.append(("genebankacc", scopes))
    if "genebankaccession" in columns:
        identifier_scopes.append(("genebankaccession", scopes))
    if "genbankaccession" in columns:
        identifier_scopes.append(("genbankaccession", scopes))
    if "gblist" in columns:
        identifier_scopes.append(("gblist", scopes))
    if "compositesequencebiosequencedatabaseentry[genbankaccession]" in columns:
        identifier_scopes.append(("compositesequencebiosequencedatabaseentry[genbankaccession]", scopes))

    # Syms
    scopes = "symbol"
    if "gene" in columns:
        identifier_scopes.append(("gene", scopes))
    if "genesymbol" in columns:
        identifier_scopes.append(("genesymbol", scopes))
    if "genename" in columns:
        identifier_scopes.append(("genename", scopes))
    if "compositesequencebiosequencedatabaseentry[genesymbol]" in columns:
        identifier_scopes.append(("compositesequencebiosequencedatabaseentry[genesymbol]", scopes))
    return identifier_scopes


def query_gb(accs):
    import sys
    from Bio import Entrez

    # define email for entrez login
    db = "nucest"
    Entrez.email = "some_email@somedomain.com"
    batchSize = 100
    retmax = 10 ** 9


    # first get GI for query accesions
    sys.stderr.write("Fetching %s entries from GenBank: %s\n" % (len(accs), ", ".join(accs[:10])))
    query = " ".join(accs)
    handle = Entrez.esearch(db=db, term=query, retmax=retmax)
    giList = Entrez.read(handle)['IdList']
    sys.stderr.write("Found %s GI: %s\n" % (len(giList), ", ".join(giList[:10])))
    # post NCBI query
    search_handle = Entrez.epost(db=db, id=",".join(giList))
    search_results = Entrez.read(search_handle)
    webenv, query_key = search_results["WebEnv"], search_results["QueryKey"]
    # fecth all results in batch of batchSize entries at once
    for start in range(0, len(giList), batchSize):
        sys.stderr.write(" %9i" % (start + 1,))
        # fetch entries in batch
        handle = Entrez.efetch(db=db, rettype="gb", retstart=start, retmax=batchSize, webenv=webenv,
                               query_key=query_key)
        #print output to stdout
        sys.stdout.write(handle.read())


def insert_myGenes():
    import pandas as pd, glob

    query = Platform.identifier == None
    count = db(query).count()
    for i, row in enumerate(db(query).select(orderby=Platform.id)):
        # try:
        platform_id = row.id
        gpl_name = row.gpl_name
        identifier = db.platform[platform_id].identifier
        if identifier:  # allow concurrent runs
            print  "Skipping", gpl_name, "Already done with", identifier
            continue

        # if gpl_name <> "GPL561":
        # continue
        print "%s/%s) Processing %s..." % (i, count, gpl_name)
        if gpl_name in (
                "GPL9052",  # "NO PLATFORM DESCRIBED"
                "GPL9115",  # "NO PLATFORM DESCRIBED"
                "GPL10999",  # "NO PLATFORM DESCRIBED"
        ):
            continue

        # if gpl_name in (
        # # "GPL8119",  # blat parsing later
        # "GPL5060",  # mygene - BadStatusLine: ''
        # "GPL10999",  # pandas - CParserError: Passed header=0 but only 0 lines in file
        # "GPL13290",  # pandas - CParserError: Passed header=0 but only 0 lines in file
        # "GPL9052",  # pandas - CParserError: Passed header=0 but only 0 lines in file
        # "GPL11326",  # pandas - ValueError: cannot insert probe, already exists
        # "GPL11327",  # pandas - ValueError: cannot insert probe, already exists
        # "GPL9115",  # pandas CParserError: Passed header=0 but only 0 lines in file
        # "GPL4803"):  # pandas - ValueError: cannot insert probe, already exists
        # print "ERROR: SKIPPING for now!"
        # continue
        # if Platform[platform_id].identifier:
        # # if db(Platform_Probe.platform_id == platform_id).count():
        # print "Already done!"
        # continue




        gpl_filenames, columns, identifier_scopes = get_probe_info(gpl_name)
        myGenes_count = 0
        for identifier, scopes in identifier_scopes:
            probes = get_probes_from_files(gpl_filenames)
            probes.columns = columns
            probes = probes.dropna(subset=[identifier])

            filename = "%s.%s.%s.mygene.csv" % (gpl_name, identifier, scopes)
            if not glob.glob(filename):
                print "Querying MyGene for %s as %s" % (identifier, scopes)
                myGene_probes = get_myGene_probes(gpl_name, probes, identifier, scopes)
                myGene_probes['platform_id'] = platform_id
                myGene_probes.index.name = 'probe'
                myGene_probes.to_csv(filename)
                print "Done MyGene"
            else:
                print "Reading from %s" % filename
                myGene_probes = pd.read_csv(filename, index_col=0)
                myGene_probes.index.name = 'probe'

            print "Dropping missing genes...",
            myGenes = myGene_probes.reset_index()[['platform_id', 'probe', 'myGene_sym', 'myGene_entrez']].dropna()
            myGenes_count = len(myGenes.index)
            if myGenes_count:
                print "Done!"
                print "Postgres inserting %s probes..." % myGenes_count
                rows = myGenes.T.to_dict().values()
                Platform_Probe.bulk_insert(rows)
                datafile = gpl_filenames[0]
                db(Platform.id == platform_id).update(scopes=scopes, identifier=identifier, datafile=datafile)
                db.commit()
                print "done %s probes!" % myGenes_count
                break
            else:
                print "No Genes Found!"
        if not myGenes_count:
            print "OOPS: No valids identifiers found for", gpl_name

    create_indices_on_postgres([('platform_probe', 'platform_id')], unique=False)


def create_sample_attribute_header_view():
    view_name = "sample_attribute_header"
    print "creating view", view_name
    db.executesql("DROP TABLE IF EXISTS %s CASCADE;" % view_name)
    db.executesql("DROP SEQUENCE IF EXISTS %s_sequence CASCADE;" % view_name)
    db.executesql("CREATE SEQUENCE %s_sequence;" % view_name)

    sql = """CREATE TABLE sample_attribute_header AS
                SELECT
                  nextval('sample_attribute_header_sequence') as id, *
                FROM
                  (SELECT
                     header,
                     num
                   FROM
                     (
                       (
                         SELECT
                           header        AS header,
                           count(header) AS num
                         FROM series_tag
                         GROUP BY header
                         ORDER BY num DESC
                       )
                       UNION (
                         SELECT DISTINCT
                           attribute_name AS header,
                           0              AS num
                         FROM sample_attribute
                         WHERE attribute_name NOT IN
                               (
                                 SELECT
                                   DISTINCT ON (header)
                                   header
                                 FROM series_tag
                               )
                         ORDER BY header
                       )
                     ) AS subquery
                   ORDER BY num DESC) AS sorted;
    """
    print sql
    db.executesql(sql)
    print "Done"


def create_series_attribute_header_view():
    view_name = "series_attribute_header"
    print "creating view", view_name
    db.executesql("DROP TABLE IF EXISTS %s CASCADE;" % view_name)
    db.executesql("DROP SEQUENCE IF EXISTS %s_sequence CASCADE;" % view_name)
    db.executesql("CREATE SEQUENCE %s_sequence;" % view_name)

    sql = """CREATE TABLE series_attribute_header AS
                SELECT
                  nextval('series_attribute_header_sequence') as id,
                  *
                FROM
                  (
                    SELECT DISTINCT
                      attribute_name AS header
                    FROM series_attribute
                    ORDER BY header) AS subquery
    """
    print sql
    db.executesql(sql)
    print "Done"


def update_counts():
    Count.truncate()
    Count.insert(what='analysis', count=db(Analysis).count())
    Count.insert(what='platform', count=db(Platform).count())
    Count.insert(what='platform_probe', count=db(Platform_Probe).count())
    Count.insert(what='tag', count=db(Tag).count())
    Count.insert(what='sample', count=db(Sample).count())
    Count.insert(what='sample_attribute', count=db(Sample_Attribute).count())
    Count.insert(what='sample_tag', count=db(Sample_Tag).count())
    Count.insert(what='series', count=db(Series).count())
    Count.insert(what='series_attribute', count=db(Series_Attribute).count())
    Count.insert(what='series_tag', count=db(Series_Tag).count())
    Count.insert(what='auth_user', count=db(db['auth_user']).count())
    db.commit()


def get_stats():
    from gluon.storage import Storage

    stats = Storage([(row.what + "_count", "{:,}".format(row.count)) for row in db(Count).select()])
    return stats


def update_tag_count():
    rec = db(Count.what == 'tag').select().first().id
    Count.update_or_insert(id, count=db(Tag).count())
    return

def mass_update_annotation(csvFilename):
    import pandas as pd
    df = pd.read_csv(csvFilename, index_col=0, dtype = {'annotation':str})
    df = df.where((pd.notnull(df)), None) #http://stackoverflow.com/questions/14162723/replacing-pandas-or-numpy-nan-with-a-none-to-use-with-mysqldb
    clean = df.drop_duplicates(['series_id', 'platform_id', 'sample_id'])
    groups = clean.groupby(['series_id', 'platform_id', 'tag_id', 'header', 'regex'])
    for (series_id, platform_id, tag_id, header, regex), df in groups:
        print series_id, platform_id
        series_tag_id = Series_Tag.insert(series_id = series_id,
                                          platform_id = platform_id,
                                          tag_id = tag_id,
                                          header = header,
                                          regex = None,
                                          created_by = 1,
                                          modified_by = 1)

        df = df[['sample_id', 'annotation']]
        df['series_tag_id'] = int(series_tag_id)
        df['created_by'] = 1
        df['modified_by'] = 1
        rows = df.T.to_dict().values()
        print "Inserting..."
        Sample_Tag.bulk_insert(rows)
        db.commit()

if __name__ == '__main__':
    mass_update_annotation("age_results.csv")
    1/0
    update_counts()
    1/0
    get_sample_tag_cross_tab()
    1 / 0
    # get_sample_tag_cross_tab()
    # 1/0
    create_sample_attribute_header_view()
    create_series_attribute_header_view()
    db.commit()
    1 / 0
    # insert_attributes()
    # insert_myGenes()
    # get_series_cross_tab()
    get_sample_cross_tab()
    # get_sample_tag_cross_tab()

