# coding: utf8
response.generic_patterns = ['.html']

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


def listed(text):
    return text or ""
    # if text:
    # fields = text.split("|||")
    # if len(fields) > 1:
    # text = UL(*[LI(field)
    # for field in fields])
    # else: text = ""
    # return text


Series_View = db.define_table('series_view',
                              Field('id', 'id', readable=False, writable=False),
                              Field('series_id', 'reference series', writable=False),
                              *[Field(field, 'text',
                                      represent=lambda value, row: listed(value))
                                for field in session.all_series_attribute_names],
                              migrate=False)

Series_Filter = db.define_table('series_filter',
                                Field('id', 'id', readable=False, writable=False),
                                migrate='series_filter.table')

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
                                      represent=lambda value, row: listed(value))
                                for field in session.all_sample_field_names],
                              migrate=False)

Sample_Filter = db.define_table('sample_filter',
                                Field('id', 'id', readable=False, writable=False),
                                migrate='sample_filter.table')

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
                                          represent=lambda value, row: listed(value))
                                    for field in session.all_sample_tag_names],
                                  migrate=False)

Series_Tag_View = db.define_table('series_tag_view',
                                  Field('id', 'id', readable=False, writable=False),
                                  Field('series_id', 'reference series', writable=False),
                                  Field('platform_id', 'reference platform', writable=False),
                                  Field('samples', 'integer', writable=False),
                                  *[Field(field, 'integer',
                                          writable=False,
                                          represent=lambda value, row: listed(value))
                                    for field in session.all_sample_tag_names],
                                  migrate=False)


# def search_form(self, url):
# form = \
# FORM(
# DIV(
# INPUT(_name="keywords",
# _id="keywords",
# _value=request.vars.keywords or "",
# _type="text",
# _class="form-control",
# _placeholder="Search...",
# _style="""width: 98%;
# z-index: 0;
#                              """
#                 ),
#                 SPAN(_class='glyphicon glyphicon-remove-circle',
#                      _onclick="clearInputField()",
#                      _style="""
#                      position: relative;
#                      left: -25px;
#                      top: 15px;
#                      z-index: 1;
#                      """,
#                      # _style=""" position: absolute;
#                      # right: 5px;
#                      # top: 0;
#                      # bottom: 0;
#                      # height: 14px;
#                      # margin: auto;
#                      # font-size: 14px;
#                      # cursor: pointer;
#                      #            color: #ccc;""",
#                 ),
#
#
#                 SPAN(
#                     BUTTON("Go",
#                            _type="submit",
#                            _style="""left: -25px;
#                                  position: relative;""",
#                            _class="btn btn-default"),
#                     _class="input-group-btn"
#                 ),
#                 _class="input-group input-group-lg"
#             ),
#             _method="GET",
#             _action=url
#         )
#     return form


def search_form(self, url):
    form = \
        FORM(
            DIV(
                DIV(
                    DIV(
                        DIV(
                            SPAN(BUTTON(I(_class="fa fa-times-circle"),
                                        _class="btn btn-default",
                                        _type="submit",
                                        _onclick="$('#keywords').val('');"),
                                 _class="input-group-btn"),
                            INPUT(_name="keywords",
                                  _id="keywords",
                                  _value=request.vars.keywords or "",
                                  _type="text",
                                  _class="form-control",
                                  _placeholder="Search..."),
                            SPAN(BUTTON(I(_class="fa fa-search"),
                                        _class="btn btn-default",
                                        _type="submit"),
                                 _class="input-group-btn"),

                            _class="input-group input-group-lg"),
                        _class="col-lg-10 col-lg-offset-1")),
                _class="row"),
            _method="GET",
            _action=url
        )
    return form


response.files += ["https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css",
                   "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css",
                   "https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"]

# def search_form(self, url):
# form = FORM(INPUT(_name='keywords',
# _value=request.get_vars.keywords,
# # _style='width:200px;',
# _id='keywords'),
# INPUT(_name='filter', _type='checkbox', _value="on", _checked=request.get_vars.filter),
# INPUT(_type='submit', _value=T('Search')),
# INPUT(_type='submit', _value=T('Clear'),
# _onclick="jQuery('#keywords').val('');"),
# _method="GET", _action=url)
#
# return form