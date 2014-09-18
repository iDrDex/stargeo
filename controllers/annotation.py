__author__ = 'dex'


def getTags(row):
    sample_id = row.sample_tag_view.sample_id if 'sample_tag_view' in row else row.sample_id
    series_id = row.sample_tag_view.series_id if 'sample_tag_view' in row else row.series_id
    platform_id = row.sample_tag_view.platform_id if 'sample_tag_view' in row else row.platform_id
    tags = [row.sample_tag_view.tag_name
            if 'sample_tag_view' in row
            else row.tag_name
            for row in db((Series_Tag.series_id == series_id) &
                          (Series_Tag.platform_id == platform_id) &
                          (Series_Tag.tag_id == Tag.id) &
                          (Sample_Tag.series_tag_id == Series_Tag.id) &
                          (Sample_Tag.sample_id == sample_id)).select(Tag.tag_name,
                                                                      distinct=Series_Tag.tag_id)]
    return DIV([DIV(tag, _class="badge") for tag in tags])


def getButton(row):
    return A(BUTTON('Tag'),
             _href=URL("tag", "index",
                       # args=row.series_tag_view.series_id,
                       vars=dict(
                           series_id=row.sample_tag_view.series_id
                           if 'sample_tag_view' in row
                           else row.series_id
                       )))


def index():
    return dict(grid=SQLFORM.grid(Sample_Tag_View,
                                  search_widget=None,
                                  buttons_placement='left',
                                  links=[lambda row: getTags(row),
                                         lambda row: getButton(row)]
    ))