# coding: utf8

Series = db.define_table('series',
                         Field('id', 'id', readable=False, writable=False),
                         Field('gse_name', 'text', writable=False),
                         format='%(gse_name)s',
                         migrate='series.table'
)

Series_Attribute = db.define_table('series_attribute',
                                   Field('id', 'id', readable=False, writable=False),
                                   Field('series_id', 'reference series', writable=False),
                                   Field('attribute_name', 'text', writable=False),
                                   Field('attribute_value', 'text', writable=False),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='series_attribute.table'
)

session.all_series_attribute_names = session.all_series_attribute_names or [row.attribute_name
                                                                            for row in
                                                                            db().select(Series_Attribute.attribute_name,
                                                                                        distinct=True,
                                                                                        orderby=Series_Attribute.attribute_name)]

# https://groups.google.com/forum/#!topic/web2py/EzC_V9pyV6s
# not needed since its views I'm custom searching
# from gluon.dal import SQLCustomType
# tsv = SQLCustomType(type='text', native='tsvector')


Series_View = db.define_table('series_view',
                              Field('id', 'id', readable=False, writable=False),
                              # Field('doc', tsv, readable=False, writable=False),
                              Field('series_id', 'reference series', writable=False),
                              *[Field(field, 'text',
                                      represent=lambda value, row: value or "")
                                for field in session.all_series_attribute_names],
                              migrate=False)


Search = db.define_table('search',
                      Field('id', 'id', readable=False, writable=False),
                      Field('fts_query', unique=True),
                      format='%(fts_query)s',
                      migrate='search.table'
)


User_Search = db.define_table('user_search',
                      Field('id', 'id', readable=False, writable=False),
                      Field('search_id', "reference search"),
                      Field('keywords'),
                      Field('fts_query'),
                      auth.signature,
                      format='%(search_id)s_%(auth_user_id)s',
                      migrate='user_search.table'
)

Series_View_Results = db.define_table('series_view_results',
                      Field('id', 'id', readable=False, writable=False),
                      Field('series_view_id', 'integer'),
                      Field('search_id', 'reference search'),
                      format='%(keywords)s',
                      migrate='series_view_results.table'
)


Platform = db.define_table('platform',
                           Field('id', 'id', readable=False, writable=False),
                           Field('gpl_name', 'text', writable=False),
                           format='%(gpl_name)s',
                           migrate='platform.table'
)

Platform_Attribute = db.define_table('platform_attribute',
                                     Field('id', 'id', readable=False, writable=False),
                                     Field('platform_id', 'reference platform', writable=False),
                                     Field('attribute_name', 'text', writable=False),
                                     Field('attribute_value', 'text', writable=False),
                                     format='%(attribute_name)s_%(attribute_value)s',
                                     migrate='platform_attribute.table'
)

Sample = db.define_table('sample',
                         Field('id', 'id', readable=False, writable=False),
                         Field('series_id', 'reference series', writable=False),
                         Field('platform_id', 'reference platform', writable=False),
                         Field('gsm_name', 'text', writable=False),
                         format='%(gsm_name)s',  # _%(series_id.gse_name)s_%(platform_id.gpl_name)s',
                         migrate='sample.table'
)

Sample_Attribute = db.define_table('sample_attribute',
                                   Field('id', 'id', readable=False, writable=False),
                                   Field('sample_id', 'reference sample', requires=None),
                                   Field('attribute_name', 'string', 256),
                                   Field('attribute_value', 'text', writable=False),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='sample_attribute.table'
)

session.all_sample_field_names = session.all_sample_field_names or [row.attribute_name
                                                                    for row in
                                                                    db().select(Sample_Attribute.attribute_name,
                                                                                distinct=True,
                                                                                orderby=Sample_Attribute.attribute_name)]

Sample_View = db.define_table('sample_view',
                              Field('id', 'id', readable=False, writable=False),
                              Field('series_id', 'reference series', writable=False),
                              Field('platform_id', 'reference platform', writable=False),
                              Field('sample_id', 'reference sample', writable=False),
                              *[Field(field, 'text',
                                      writable=False,
                                      represent=lambda value, row: value or "")
                                for field in session.all_sample_field_names],
                              migrate=False)

Sample_View_Results = db.define_table('sample_view_results',
                      Field('id', 'id', readable=False, writable=False),
                      Field('sample_view_id', 'integer'),
                      Field('search_id', 'reference search'),
                      format='%(keywords)s',
                      migrate='sample_view_results.table'
)


Tag = db.define_table('tag',
                      Field('id', 'id', readable=False, writable=False),
                      Field('tag_name', unique=True),
                      Field('description'),
                      auth.signature,
                      format='%(tag_name)s',
                      migrate='tag.table'
)



Series_Tag = db.define_table('series_tag',
                             Field('id', 'id', readable=False, writable=False),
                             Field('series_id', 'reference series', ),
                             Field('platform_id', 'reference platform'),
                             Field('tag_id', 'reference tag'),
                             Field('header'),
                             Field('regex', requires=IS_NOT_EMPTY()),
                             Field('show_invariant', 'boolean'),
                             auth.signature,
                             format='%(tag_id.tag_name)s_%(series_id.gse_name)s_%(platform_id.gpl_name)s',
                             migrate="series_tag.table"
)

Sample_Tag = db.define_table('sample_tag',
                             Field('id', 'id', readable=False, writable=False),
                             Field('sample_id', 'reference sample', writable=False),
                             Field('series_tag_id', 'reference series_tag', writable=False),
                             Field('annotation', 'text'),
                             auth.signature,
                             format='%(annotation)s',
                             migrate="sample_tag.table"
)

Sample_View_Annotation_Filter = db.define_table('sample_view_annotation_filter',
                                                Field('id', 'id', readable=False, writable=False),
                                                Field('sample_view_id', writable=False),  # 'reference sample_view'
                                                Field('annotation', 'text'),
                                                Field('session_id', default=response.session_id,
                                                      readable=False, writable=False),
                                                Field('created_on', 'datetime', default=request.now, readable=False,
                                                      writable=False),
                                                format='%(annotation)s',
                                                migrate="sample_view_annotation_filter.table"
)

session.all_sample_tag_names = session.all_sample_tag_names or [row.tag_name
                                                                for row in
                                                                db().select(Tag.tag_name,
                                                                            distinct=True,
                                                                            orderby=Tag.tag_name)]

Sample_Tag_View = db.define_table('sample_tag_view',
                                  Field('id', 'id', readable=False, writable=False),
                                  Field('series_id', 'reference series', writable=False),
                                  Field('platform_id', 'reference platform', writable=False),
                                  Field('sample_id', 'reference sample', writable=False),
                                  *[Field(field, 'text',
                                          writable=False,
                                          represent=lambda value, row: value or "")
                                    for field in session.all_sample_tag_names],
                                  migrate=False)

Series_Tag_View = db.define_table('series_tag_view',
                                  Field('id', 'id', readable=False, writable=False),
                                  Field('series_id', 'reference series', writable=False),
                                  Field('platform_id', 'reference platform', writable=False),
                                  Field('samples', 'integer', writable=False),
                                  *[Field(field, 'integer',
                                          writable=False,
                                          represent=lambda value, row: value or "")
                                    for field in session.all_sample_tag_names],
                                  migrate=False)

