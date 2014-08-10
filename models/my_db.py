__author__ = 'dex'
response.generic_patterns = ['.html']

Series = db.define_table('series',
                         Field('gse_name'),
                         format='%(gse_name)s',
                         migrate='series.table'
)

Series_Attribute = db.define_table('series_attribute',
                                   Field('series_id', 'reference series'),
                                   Field('name'),
                                   Field('value', 'text'),
                                   format='%(name)s_%(value)s',
                                   migrate='series_attribute.table'
)

Platform = db.define_table('platform',
                           Field('gpl_name'),
                           format='%(gpl_name)s',
                           migrate='platform.table'
)

Platform_Attribute = db.define_table('platform_attribute',
                                     Field('platform_id', 'reference platform'),
                                     Field('name'),
                                     Field('value', 'text'),
                                     format='%(name)s_%(value)s',
                                     migrate='platform_attribute.table'
)

Sample = db.define_table('sample',
                         Field('series_id', 'reference series'),
                         Field('platform_id', 'reference platform'),
                         Field('gsm_name'),
                         format='%(gsm_name)s',
                         migrate='sample.table'
)

Sample_Attribute = db.define_table('sample_attribute',
                                   Field('sample_id', 'reference sample'),
                                   Field('name', 'text'),
                                   Field('value', 'text'),
                                   format='%(name)s_%(value)s',
                                   migrate='sample_attribute.table'
)

Annotation = db.define_table('annotation',
                             Field('name', unique=True),
                             Field('description'),
                             auth.signature,
                             format='%(name)s',
                             migrate='annotation.table'
)

Series_Annotation = db.define_table('series_annotation',
                                    Field('series_id', 'reference series'),
                                    Field('platform_id', 'reference platform'),
                                    Field('annotation_id', 'reference annotation'),
                                    Field('col'),
                                    Field('regex'),
                                    auth.signature,
                                    format='%(col)s_%(regex)s',
                                    migrate="series_annotation.table"
)

Sample_Annotation = db.define_table('sample_annotation',
                                    Field('series_annotation_id', 'reference series_annotation'),
                                    Field('sample_id', 'reference sample'),
                                    Field('feature'),
                                    auth.signature,
                                    format='%(feature)s',
                                    migrate="sample_annotation.table"
)

Search = db.define_table('search',
                         Field('query'),
                         format='%(query)s',
                         migrate='search.table'
)


def create_unique_indices_on_postgres(indices):
    # Modified from http://osdir.com/ml/web2py/2010-09/msg00952.html
    '''Creates a set of indices if they do not exist'''
    # # Edit this list of table columns to index
    # # The format is [('table', 'column1, column2, ...'),...]
    print "checking indicies:"
    for table, columns in indices:
        relname = "%s_%s_idx"%(table.replace("_", ""),
                               columns.replace(" ", "") \
                               .replace("_", "") \
                               .replace(",", "_"))
        print "\t%s" % relname,
        index_exists = \
            db.executesql("select count(*) from pg_class where relname = '%s';" % relname)[0][0] == 1
        if not index_exists:
            print "CREATING"
            db.executesql('create unique index %s on %s (%s);'
                          % (relname, table, columns))
        db.commit()
        print "OK"
    print "Done!"


create_unique_indices_on_postgres([('sample_attribute', 'sample_id, name')])
