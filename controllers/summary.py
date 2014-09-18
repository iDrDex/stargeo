__author__ = 'dex'


def getButton(row):
    return A(BUTTON('Tag'),
             _href=URL("tag", "index",
                       # args=row.series_tag_view.series_id,
                       vars=dict(
                           series_id=row.series_tag_view.series_id
                           if 'series_tag_view' in row
                           else row.series_id
                       )))


def getTags(row):
    series_id = row.series_tag_view.series_id if 'series_tag_view' in row else row.series_id
    tags = [row.series_tag_view.tag_name
            if 'series_tag_view' in row
            else row.tag_name
            for row in db((Series_Tag.series_id == series_id) &
                          # (Series_Tag.platform_id == row.platform_id) &
                          (Series_Tag.tag_id == Tag.id)).select(Tag.tag_name,
                                                                distinct=Series_Tag.tag_id)]
    return DIV([DIV(tag, _class="badge") for tag in tags])


def index():
    return dict(grid=SQLFORM.grid(Series_Tag_View,
                                  search_widget=None,
                                  buttons_placement='left',
                                  links=[lambda row: getTags(row),
                                         lambda row: getButton(row)]

    ))
