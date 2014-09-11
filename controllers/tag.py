__author__ = 'dex'


def add():
    form = SQLFORM(Tag)
    if form.process().accepted:
        request.get_vars.tag_id = form.vars.id
        redirect(URL('index', vars=request.get_vars))
    return dict(form=form)


def index():
    series_id = request.vars.series_id or request.env.http_referrer and redirect(request.env.http_referrer)
    # Series_Tag.series_id.default = series_id
    Series_Tag.series_id.writable = False
    ids = [row.platform_id for row in db(Sample.series_id == series_id) \
        .select(Sample.platform_id, distinct=True)]
    labels = [Platform[id].gpl_name for id in ids]
    Series_Tag.platform_id.requires = IS_IN_SET([""] + ids, labels=["all"] + labels, zero=None)
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

    fields = [Sample_View[header] for header in headers]
    if request.vars.header:
        fields = [Sample_View[request.vars.header]]
    else:
        fields = [Sample_View[header] for header in headers]

    fields = [Sample_View['sample_id'],
              Sample_View['platform_id']] + fields
    for field in Series_Tag.fields:
        Series_Tag[field].default = request.get_vars[field] \
            if field in request.get_vars \
            else request.vars[field]

    form = SQLFORM(Series_Tag, _id='form', hidden=dict(toSubmit=True))

    form.custom.submit['_class'] = 'submit'  # have to use _class for jquery because _name or _id breaks the grid view
    form.add_button('+', URL('add', vars=request.get_vars))

    if request.vars.toSubmit:
        if form.validate(formname='form'):
            redirect(URL('annotate', 'index'))

    # print field_names
    return dict(form=form,
                grid=SQLFORM.grid(query, search_widget=None,
                                  fields=fields,
                                  maxtextlength=1000,
                                  formname='form')
    )
    # request.vars.series_id


def a():
    return dict(a="adsf")