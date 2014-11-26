__author__ = 'dex'


@auth.requires_login()
def add():
    form = SQLFORM(Tag)
    form.add_button("Cancel", URL('index', vars=request.get_vars))
    if form.process().accepted:
        # get_sample_tag_cross_tab()  # rebuild tables
        if 'tag_form_vars' not in session:
            session.tag_form_vars = form.vars
        else: session.tag_form_vars.tag_id = form.vars.id
        update_tag_count()
        redirect(URL('index', vars=request.get_vars))
    return dict(form=form)


def __update_form_model():
    # build a new form model for this series_id if none exists or new series_id

    from gluon.storage import Storage

    if not session.tag_form_vars:
        session.tag_form_vars = Storage()

    # update model from requests if same series_id
    for var in Series_Tag.fields[1:]:
        if var not in auth.signature:
            if var in request.vars:
                session.tag_form_vars[var] = request.vars[var]
    # checkboxes are special case as only requested when checked
    session.tag_form_vars.show_invariant = request.vars.show_invariant

    # update the defaults based on form model
    for var in Series_Tag.fields[1:]:
        Series_Tag[var].default = session.tag_form_vars[var]


def index():
    __update_form_model()

    # SERIES
    series_id = session.tag_form_vars.series_id or redirect(URL('default', 'index'))


    # PLATFORM
    ids = [row.platform_id for row in db(Sample.series_id == series_id) \
        .select(Sample.platform_id, distinct=True)]
    labels = [Platform[id].gpl_name for id in ids]
    Series_Tag.platform_id.requires = IS_IN_SET([""] + ids, labels=["all"] + labels, zero=None)

    # TAGS
    query = (Series_Tag.series_id == series_id) & (Tag.id == Series_Tag.tag_id)
    if session.tag_form_vars.platform_id:
        query &= (Series_Tag.platform_id == request.get_vars.platform_id)
    tag_ids = set(row.id
                  for row in
                  db(query).select(Tag.id, distinct=True))
    ids = [row.id for row in db(Tag).select(Tag.id, distinct=True) if row.id not in tag_ids]

    # https://web2py.wordpress.com/category/web2py-validators/
    # IS_IN_DB() professional usage to filter is_in_db because is_in_set allows nulls
    Series_Tag.tag_id.requires = IS_IN_DB(db(Tag.id.belongs(ids)),
                                          Tag.id,
                                          Tag._format)

    # HEADERS
    query = Sample_View.series_id == series_id

    # if request.vars.header:
    # fields = [Sample_View[request.vars.header]]
    # else:
    fields = get_variant_fields(query=query, paginate=paginate, view=Sample_View) \
        if not request.vars.show_invariant else None

    session.tag_form_vars.headers = [field.name for field in fields] if fields else Sample_View.fields[1:]
    Series_Tag.header.requires = IS_IN_SET([""] + session.tag_form_vars.headers,
                                           labels=["all"] + session.tag_form_vars.headers,
                                           zero=None)

    query = Sample_View.series_id == series_id
    # fields = None#[Sample_View[request.vars.header]] if request.vars.header else None
    form = SQLFORM(Series_Tag,
                   _id='form',
                   submit_button="Continue",
                   formname='form',
                   hidden=dict(series_id=series_id))

    form.custom.submit['_class'] = 'submit'  # have to use _class for jquery because _name or _id breaks the grid view
    form.add_button('+', URL('add', vars=request.get_vars))
    # form.add_button("Cancel", URL('default', 'index', vars=request.get_vars))

    if form.validate(formname='form'):
        # # update form model
        # for var in form.vars:
        # session.tag_form_vars[var] = form.vars[var]
        if 'page' in request.get_vars:
            del (request.get_vars.page)
        redirect(URL('annotate', 'index', vars=request.get_vars))

    grid = SQLFORM.grid(query,
                        search_widget=None,
                        searchable=None,
                        fields=[Sample_View.sample_id,
                                Sample_View[Series_Tag.header.default]] \
                            if Series_Tag.header.default in session.tag_form_vars.headers
                        else fields,
                        maxtextlength=1000,
                        create=False,
                        editable=False,
                        deletable=False,
                        user_signature=None,
                        formname='form',
                        buttons_placement='left',
                        links=[lambda row: get_tags(row)])
    grid = DIV(grid,
               SCRIPT('''   $("#series_tag_show_invariant").change(function () {
                                $('#series_tag_header').val("")
                                $("input[name='_formkey']").val("")
                                $('#form').submit()
                            });
               '''),
               SCRIPT('''   $("#series_tag_header").change(function () {
                                $("input[name='_formkey']").val("")
                                $('#form').submit()
                            });'''
               ),
    )

    return dict(form=form,
                grid=grid)