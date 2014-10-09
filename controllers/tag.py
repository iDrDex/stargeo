__author__ = 'dex'


@auth.requires_login()
def add():
    form = SQLFORM(Tag)
    form.add_button("Cancel", URL('index', vars = request.get_vars))
    if form.process().accepted:
        getSampleTagCrossTab()  # rebuild tables
        session.tag_form_vars.tag_id = form.vars.id
        redirect(URL('index'))
    return dict(form=form)


def __update_model():
    from gluon.storage import Storage
    if not session.tag_form_vars:
        session.tag_form_vars = Storage()

    # update form model based on requests if present
    for var in Series_Tag.fields[1:]:
        if var not in auth.signature:
            if var in request.vars:
                session.tag_form_vars[var] = request.vars[var]
    # checkboxes are special case as only requested when checked
    session.tag_form_vars.show_invariant = request.vars.show_invariant

    #update the defaults based on form model
    for var in Series_Tag.fields[1:]:
        Series_Tag[var].default = session.tag_form_vars[var]




def index():

    __update_model()

    series_id = session.tag_form_vars.series_id or redirect('default', 'index')
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
        if not session.tag_form_vars.show_invariant else None

    session.tag_form_vars.headers = [field.name for field in fields] if fields else Sample_View.fields[1:]
    Series_Tag.header.requires = IS_IN_SET([""] + session.tag_form_vars.headers,
                                           labels=["all"] + session.tag_form_vars.headers,
                                           zero=None)

    Series_Tag.series_id.writable = False
    form = SQLFORM(Series_Tag,
                   _id='form')

    form.custom.submit['_class'] = 'submit'  # have to use _class for jquery because _name or _id breaks the grid view
    form.add_button('+', URL('add'))

    if form.validate(formname='form'):
        # update form model
        for var in form.vars:
            session.tag_form_vars[var] = form.vars[var]
        if 'page' in request.get_vars:
            del(request.get_vars.page)

        redirect(URL('annotate', 'index', vars=request.get_vars))

    grid = SQLFORM.grid(query,
                        search_widget=None,
                        searchable=None,
                        fields=[Sample_View[session.tag_form_vars.header]] if session.tag_form_vars.header else fields,
                        maxtextlength=1000,
                        create=False,
                        editable=False,
                        deletable=False,
                        user_signature=None,
                        buttons_placement='left',
                        formname='form')

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

    # print field_names
    return dict(form=form,
                grid=grid
    )
