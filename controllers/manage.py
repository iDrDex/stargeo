__author__ = 'dex'


def tag():


    from gluon.tools import Crud
    crud = Crud(db)

    grid = SQLFORM.grid(Tag,
                        orderby=~Tag.id,
                        maxtextlength=100,
                        ondelete=update_sample_cross_tab,
                        onupdate=update_sample_cross_tab,
                        oncreate=add_sample_cross_tab,)

    return dict(grid=grid)

def series_tag():
    grid = SQLFORM.grid(Series_Tag,
                        orderby=~Series_Tag.id,
                        maxtextlength=100,
                        ondelete=update_sample_cross_tab,
                        onupdate=update_sample_cross_tab,
                        oncreate=add_sample_cross_tab,)
    return dict(grid=grid)
