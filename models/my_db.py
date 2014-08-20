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
                                   Field('name', 'string', 256),
                                   Field('value', 'text'),
                                   format='%(name)s_%(value)s',
                                   migrate='sample_attribute.table'
)

# from plugin_haystack import Haystack, SolrBackend
# sample_attribute_index = Haystack(Sample_Attribute, backend=SolrBackend,url='http://localhost:8983/solr')
# sample_attribute_index.indexes('name','value')

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