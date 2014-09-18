__author__ = 'dex'

import re


def getSampleTagCrossTab():
    print "reading tag names"
    tag_names = db(Tag).select(orderby=Tag.tag_name)

    # CREATE VIEW
    print "creating view"
    db.executesql("DROP SEQUENCE IF EXISTS sample_tag_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE sample_tag_sequence;")
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS sample_tag_view ;")

    # print "creating view"
    tagsSql = "," \
        .join(["""string_agg(CASE tag_id WHEN %s THEN annotation END, '|||') as %s \n""" \
               % (row['id'], row['tag_name'])
               for row in tag_names])
    sql = """CREATE MATERIALIZED VIEW sample_tag_view AS
             SELECT NEXTVAL('sample_tag_sequence') as id, series_id, platform_id, sample_id,""" \
          + tagsSql \
          + """ FROM sample_tag
                 JOIN series_tag ON sample_tag.series_tag_id = series_tag.id
                 JOIN tag ON tag.id = tag_id
               GROUP BY series_id, platform_id, sample_id;"""
    print sql
    db.executesql(sql)

    getSeriesTagCrossTab()
    db.commit()
    session.all_sample_tag_names = None


def getSeriesTagCrossTab():
    print "reading tag names"
    tag_names = db().select(Tag.tag_name,
                            orderby=Tag.tag_name,
                            distinct=True)
    # CREATE VIEW
    print "creating view"
    db.executesql("DROP SEQUENCE IF EXISTS series_tag_sequence CASCADE;")
    db.executesql("CREATE SEQUENCE series_tag_sequence;")
    db.executesql("DROP MATERIALIZED VIEW IF EXISTS series_tag_view ;")

    # print "creating view"
    tagsSql = "," \
        .join(["""count(%s) as %s \n""" \
               % (row['tag_name'], row['tag_name'])
               for row in tag_names])
    sql = """CREATE MATERIALIZED VIEW series_tag_view AS
             SELECT NEXTVAL('series_tag_sequence') as id, series_id, platform_id,
             count(sample_id) as samples,""" \
          + tagsSql \
          + """ FROM sample_tag_view
               GROUP BY series_id, platform_id;"""
    print sql
    db.executesql(sql)
    db.commit()


def index():
    query = (Sample_View.series_id == request.vars.series_id)
    if request.vars.platform_id:
        query &= query(Sample_View.platform_id == request.vars.platform_id)

    header = request.vars.header
    annotation_type = 'text'
    regex = request.vars.regex
    if header:
        p = re.compile(regex)
        if p.groups:
            f = lambda row: p.search(row[header]) and p.search(row[header]).group(1)
        else:
            annotation_type = 'boolean'
            f = lambda row: p.search(row[header]) and True
    else:
        f = lambda row: regex

    Sample_View_Annotation_Filter.truncate()
    for row in db(query).select():
        Sample_View_Annotation_Filter.insert(
            sample_view_id=row.id,
            annotation=f(row))
    redirect(URL('edit', vars=request.get_vars))


def save():
    if request.vars.platform_id:
        platform_ids = [request.vars.platform_id]
    else:
        platform_ids = [row.platform_id
                        for row in
                        db(Sample_View.series_id == request.vars.series_id) \
                            .select(Sample_View.platform_id, distinct=True)]
    for platform_id in platform_ids:
        request.vars.platform_id = platform_id
        toInsert = dict([(var, request.vars[var]) for var in Series_Tag.fields[1:]])
        series_tag_id = Series_Tag.insert(**toInsert)
        rows = db((Sample_View_Annotation_Filter.sample_view_id == Sample_View.id) & (
            Sample_View.platform_id == platform_id)).select()
        rows = [dict(sample_id=row.sample_view.sample_id,
                     series_tag_id=series_tag_id,
                     annotation=row.sample_view_annotation_filter.annotation) for row in rows]
        Sample_Tag.bulk_insert(rows)
    # from setup_db import getSampleTagCrossTab
    getSampleTagCrossTab()
    redirect(URL('tag', 'index', vars=dict(series_id=request.vars.series_id)))


def edit():
    headers = session.headers
    fields = [Sample_View['sample_id'],
              Sample_View['platform_id'],
              Sample_View_Annotation_Filter['annotation'],
             ] + \
             ([Sample_View[request.vars.header]] if request.vars.header \
                  else [Sample_View[header] for header in headers])
    series_id = request.vars.series_id
    return dict(form=DIV(A(BUTTON('Save'), _href=URL('save', vars=request.get_vars)),
                         A(BUTTON('Cancel'), _href=URL('tag', 'index', vars=dict(series_id=series_id)))),
                grid=SQLFORM.grid(Sample_View_Annotation_Filter.sample_view_id == Sample_View.id,
                                  search_widget=None,
                                  details=False,
                                  deletable=False,
                                  editable=True,
                                  create=False,
                                  user_signature=False,
                                  fields=fields,
                                  maxtextlength=1000))


