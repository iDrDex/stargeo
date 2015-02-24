__author__ = 'dex'

import re

@auth.requires_login()
def index():
    # tag and regex must be defined
    tag_id, regex = (session.tag_form_vars.tag_id, session.tag_form_vars.regex) \
        if 'tag_form_vars' in session \
           and session.tag_form_vars.tag_id \
           and session.tag_form_vars.regex \
        else redirect(URL('default', 'index'))

    __setup_filter() #must be separate conroller from edit()
    redirect(URL('edit'))

@auth.requires_login()
def edit():
    headers = session.tag_form_vars.headers
    fields = [Sample_View['sample_id'],
              Sample_View['platform_id'],
              Sample_View_Annotation_Filter['annotation'],
             ] + \
             ([Sample_View[session.tag_form_vars.header]]
              if session.tag_form_vars.header \
                  else [Sample_View[header] for header in headers])

    # set up form defaults as readonly
    for field in Series_Tag._fields[1:]:
        Series_Tag[field].default = session.tag_form_vars[field]
        Series_Tag[field].writable = False

    form = SQLFORM(Series_Tag, submit_button="Save")
    form.add_button("Cancel", URL('tag', 'index', vars=request.get_vars))
    if form.validate():
        return __save()

    grid = SQLFORM.grid((Sample_View_Annotation_Filter.sample_view_id == Sample_View.id) & \
                        (Sample_View_Annotation_Filter.session_id == response.session_id),
                        field_id=Sample_View_Annotation_Filter.id,
                        searchable=None,
                        search_widget=None,
                        details=False,
                        deletable=False,
                        create=False,
                        # user_signature=False,
                        maxtextlength=1000 if fields else 20,
                        fields=fields)
    # grid = SQLFORM.grid(Sample_View_Annotation_Filter)
    return dict(
        form=form,
        grid=grid
    )


def __setup_filter():
    # SERIES ID
    series_id = request.vars.series_id or \
                (session.tag_form_vars.series_id \
                     if 'tag_form_vars' in session \
                     else redirect(URL('default', 'index')))

    query = (Sample_View.series_id == series_id)

    # PLATFORM
    platform_id = session.tag_form_vars.platform_id
    if platform_id:
        query &= query(Sample_View.platform_id == platform_id)

    header = session.tag_form_vars.header
    regex = session.tag_form_vars.regex
    tag_id = session.tag_form_vars.tag_id
    tag_name = Tag(tag_id).tag_name
    if header:
        p = re.compile(regex)
        if p.groups:
            # R: double search - inefficient
            f = lambda row: p.search(row[header]) and p.search(row[header]).group(1)
        else:
            annotation_type = 'boolean'
            f = lambda row: p.search(str(row[header])) and tag_name
    else:
        f = lambda row: regex

    # clean out any other concurrent annotations from current sessions
    db(Sample_View_Annotation_Filter.session_id == response.session_id).delete()

    for row in db(query).select():
        annotation = f(row)
        Sample_View_Annotation_Filter.insert(
            sample_view_id=row.id,
            annotation=annotation)


def __save():
    platform_id = session.tag_form_vars.platform_id
    platform_ids = [platform_id] if platform_id \
        else [row.platform_id
              for row in
              db(Sample_View.series_id == session.tag_form_vars.series_id) \
                  .select(Sample_View.platform_id, distinct=True)]

    for platform_id in platform_ids:
        toInsert = dict(
            [(var, session.tag_form_vars[var])
             for var in Series_Tag.fields[1:]
             if (var not in auth.signature.fields)])
        toInsert['platform_id'] = platform_id
        series_tag_id = Series_Tag.insert(**toInsert)
        rows = db((Sample_View_Annotation_Filter.sample_view_id == Sample_View.id) &
                  (Sample_View_Annotation_Filter.session_id == response.session_id) &
                  (Sample_View.platform_id == platform_id)).select()
        rows = [dict(sample_id=row.sample_view.sample_id,
                     series_tag_id=series_tag_id,
                     annotation=row.sample_view_annotation_filter.annotation) for row in rows]
        Sample_Tag.bulk_insert(rows)

    # # from setup_db import get_sample_tag_cross_tab
    # get_sample_tag_cross_tab()

    # clear the regex on the form
    session.tag_form_vars.regex = None
    session.tag_form_vars.tag_id = None

    if 'page' in request.get_vars:
        del (request.get_vars.page)
    redirect(URL('tag', 'index', vars=request.get_vars))

