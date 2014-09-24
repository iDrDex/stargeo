__author__ = 'dex'


def add():
    form = SQLFORM(Tag)
    if form.process().accepted:
        request.get_vars.tag_id = form.vars.id
        redirect(URL('index', vars=request.get_vars))
    return dict(form=form)


def index():
    # SERIES ID
    series_id = session.series_id or request.env.http_referrer and redirect(request.env.http_referrer)

    # PLATFORM
    ids = [row.platform_id for row in db(Sample.series_id == series_id) \
        .select(Sample.platform_id, distinct=True)]
    labels = [Platform[id].gpl_name for id in ids]
    Series_Tag.platform_id.requires = IS_IN_SET([""] + ids, labels=["all"] + labels, zero=None)

    # TAGS
    query = (Series_Tag.series_id == series_id) & (Tag.id == Series_Tag.tag_id)
    if request.vars.platform_id:
        query &= (Series_Tag.platform_id == request.vars.platform_id)
    tag_ids = set(row.id
                  for row in
                  db(query).select(Tag.id, distinct=True))
    # print db._lastsql
    ids = [row.id for row in db(Tag).select(Tag.id, distinct=True) if row.id not in tag_ids]
    # print db._lastsql
    labels = [Tag[id].tag_name for id in ids]
    Series_Tag.tag_id.requires = IS_IN_SET(ids, labels=labels)

    # HEADERS
    query = Sample_View.series_id == series_id

    sql = """SELECT
              attribute_name,
              count(DISTINCT attribute_value)
            FROM sample_attribute
              JOIN sample ON sample.id = sample_id
            WHERE series_id = %s
            GROUP BY attribute_name;"""

    attribute_name2count = dict(db.executesql(sql % series_id))
    # sorted([row.attribute_name
    # for row in db().select(Sample_Attribute.attribute_name, distinct=True)])
    # inset = ["all"]+sorted(attribute_name2count.keys())
    # Series_Tag.header.requires = IS_IN_SET(inset, zero=None)
    # Series_Tag.header.default = ""
    headers = sorted([attribute_name
                      for attribute_name in attribute_name2count
                      if attribute_name2count[attribute_name] > 1]) \
        if not request.vars.show_invariant else \
        sorted([attribute_name
                for attribute_name in attribute_name2count])

    Series_Tag.header.requires = IS_IN_SET([""] + headers, labels=["all"] + headers, zero=None)

    fields = [Sample_View['sample_id'],
              Sample_View['platform_id']] + \
             ([Sample_View[request.vars.header]] if request.vars.header \
                  else [Sample_View[header] for header in headers])

    for field in Series_Tag.fields:
        Series_Tag[field].default = request.vars[field][0] \
            if type(request.vars[field]) == list \
            else request.vars[field]

    form = SQLFORM(Series_Tag,
                   _id='form')

    form.custom.submit['_class'] = 'submit'  # have to use _class for jquery because _name or _id breaks the grid view
    form.add_button('+', URL('add', vars=request.get_vars))

    if form.validate(formname='form'):
        form.vars.series_id = series_id
        # return annotate(fields)
        session.headers = headers
        redirect(URL('annotate', 'index', vars=form.vars))

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
