# coding: utf8
# response.generic_patterns = ['.html']

Series = db.define_table('series',
                         Field('gse_name', 'text'),
                         format='%(gse_name)s',
                         migrate='series.table'
)

Series_Attribute = db.define_table('series_attribute',
                                   Field('series_id', 'reference series'),
                                   Field('attribute_name', 'text'),
                                   Field('attribute_value', 'text', 'text'),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='series_attribute.table'
)

session.all_series_field_names = session.all_series_field_names or [row.attribute_name
                                                            for row in
                                                            db().select(Series_Attribute.attribute_name,
                                                                        distinct=True,
                                                                        orderby=Series_Attribute.attribute_name)]
def listed(text):
    return text or ""
    # if text:
    #     fields = text.split("|||")
    #     if len(fields) > 1:
    #         text = UL(*[LI(field)
    #                   for field in fields])
    # else: text = ""
    # return text

Series_View = db.define_table('series_view',
                              Field('series_id', 'reference series'),
                              *[Field(field, 'text',
                                      represent = lambda value,row: listed(value))
                                for field in session.all_series_field_names],
                              migrate=False)

Series_Filter = db.define_table('series_filter',
                                migrate='series_filter.table')

Platform = db.define_table('platform',
                           Field('gpl_name', 'text'),
                           format='%(gpl_name)s',
                           migrate='platform.table'
)

Platform_Attribute = db.define_table('platform_attribute',
                                     Field('platform_id', 'reference platform'),
                                     Field('attribute_name', 'text'),
                                     Field('attribute_value', 'text', 'text'),
                                     format='%(attribute_name)s_%(attribute_value)s',
                                     migrate='platform_attribute.table'
)

Sample = db.define_table('sample',
                         Field('series_id', 'reference series'),
                         Field('platform_id', 'reference platform'),
                         Field('gsm_name', 'text'),
                         format='%(gsm_name)s',
                         migrate='sample.table'
)

Sample_Attribute = db.define_table('sample_attribute',
                                   Field('sample_id', 'reference sample', requires=None),
                                   Field('attribute_name', 'string', 256),
                                   Field('attribute_value', 'text', 'text'),
                                   format='%(attribute_name)s_%(attribute_value)s',
                                   migrate='sample_attribute.table'
)

session.all_sample_field_names = session.all_sample_field_names or [row.attribute_name
                                                            for row in
                                                            db().select(Sample_Attribute.attribute_name,
                                                                        distinct=True,
                                                                        orderby=Sample_Attribute.attribute_name)]

Sample_View = db.define_table('sample_view',
                              Field('series_id', 'reference series'),
                              Field('platform_id', 'reference platform'),
                              Field('sample_id', 'reference sample'),
                              *[Field(field, 'text',
                                      represent = lambda value,row:listed(value))
                                for field in session.all_sample_field_names],
                              migrate=False)

Sample_Filter = db.define_table('sample_filter',
                                migrate='sample_filter.table')



Tag = db.define_table('tag',
                             Field('tag_name', unique=True),
                             Field('description'),
                             format='%(tag_name)s',
                             migrate='tag.table'
)

Series_Tag = db.define_table('series_tag',
                                   Field('series_id', 'reference series'),
                                   Field('platform_id', 'reference platform'),
                                   Field('tag_id', 'reference tag'),
                                   Field('header'),
                                   Field('regex', requires=IS_NOT_EMPTY()),
                                   Field('show_invariant', 'boolean'),
                                   format='%(series.name)s_%(platform.name)s',
                                   migrate="series_tag.table"
)

# Sample_Tag = db.define_table('sample_tag',
#                                     Field('series_tag_id', 'reference series_tag'),
#                                    Field('sample_id', 'reference sample'),
#                                     Field('tag_value'),
#                                     format='%(series_tag)s',
#                                     migrate="sample_tag.table"
# )
