__author__ = 'dex'


@auth.requires_login()
def add():
    form = SQLFORM(Tag)
    form.add_button("Cancel", URL('index'))
    if form.process().accepted:
        session.tag_form_vars.tag_id = form.vars.id
        session.tag_count = None
        redirect(URL('index'))
    return dict(form=form)


def getHeaders(series_id, minCount=2):
    sql = """SELECT
              attribute_name
            FROM sample_attribute
              JOIN sample ON sample.id = sample_id
            WHERE series_id = %s
            GROUP BY attribute_name
            HAVING  count(DISTINCT attribute_value) >= %s
            ORDER by attribute_name;"""

    query = sql % (series_id, minCount)
    print query
    return sorted([row['attribute_name']
                   for row
                   in db.executesql(query, as_dict=True)])


def index():
    # SERIES ID
    series_id = request.vars.series_id or \
                (session.tag_form_vars.series_id \
                     if 'tag_form_vars' in session \
                     else redirect(URL('default', 'index')))

    # update form model based on requests
    for var in request.vars:
        if var in Series_Tag.fields:
            session.tag_form_vars[var] = request.vars[var]

    # set defaults based on form model
    for var in session.tag_form_vars:
        if var in Series_Tag.fields:
            Series_Tag[var].default = session.tag_form_vars[var]


    # PLATFORM
    ids = [row.platform_id for row in db(Sample.series_id == series_id) \
        .select(Sample.platform_id, distinct=True)]
    labels = [Platform[id].gpl_name for id in ids]
    Series_Tag.platform_id.requires = IS_IN_SET([""] + ids, labels=["all"] + labels, zero=None)

    # TAGS
    query = (Series_Tag.series_id == series_id) & (Tag.id == Series_Tag.tag_id)
    if request.get_vars.platform_id:
        query &= (Series_Tag.platform_id == request.get_vars.platform_id)
    tag_ids = set(row.id
                  for row in
                  db(query).select(Tag.id, distinct=True))
    ids = [row.id for row in db(Tag).select(Tag.id, distinct=True) if row.id not in tag_ids]

    # https://web2py.wordpress.com/category/web2py-validators/
    #IS_IN_DB() professional usage to filter is_in_db because is_in_set allows nulls
    Series_Tag.tag_id.requires = IS_IN_DB(db(Tag.id.belongs(ids)),
                                          Tag.id,
                                          Tag._format)

    # HEADERS
    query = Sample_View.series_id == series_id
    minCount = 1 if request.vars.show_invariant else 2
    session.headers = getHeaders(series_id, minCount)
    Series_Tag.header.requires = IS_IN_SET([""] + session.headers,
                                           labels=["all"] + session.headers,
                                           zero=None)

    fields = [Sample_View['sample_id'],
              Sample_View['platform_id']] + \
             ([Sample_View[request.vars.header]] if request.vars.header \
                  else [Sample_View[header] for header in session.headers])

    form = SQLFORM(Series_Tag,
                   _id='form')

    form.custom.submit['_class'] = 'submit'  # have to use _class for jquery because _name or _id breaks the grid view
    form.add_button('+', URL('add', vars=request.get_vars))

    if form.validate(formname='form'):
        # update form model
        for var in form.vars:
            session.tag_form_vars[var] = form.vars[var]

        redirect(URL('annotate', 'index'))

    grid = SQLFORM.grid(query, search_widget=None,
                        fields=fields,
                        maxtextlength=1000,
                        formname='form')
    grid = DIV(grid,
               SCRIPT('''   $("#series_tag_show_invariant").click(function () {
                                $('#series_tag_header').val("")
                                $("input[name='_formkey']").val("")
                                $('#form').submit()
                            });
               '''),
               SCRIPT('''   $("#series_tag_header").change(function () {
                                $('#series_tag_show_invariant').val("")
                                $("input[name='_formkey']").val("")
                                $('#form').submit()
                            });'''
               ),
    )

    # print field_names
    return dict(form=form,
                grid=grid
    )
