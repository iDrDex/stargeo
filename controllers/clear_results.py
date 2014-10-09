__author__ = 'dex'
def index():
    Series_View_Results.truncate()
    Sample_View_Results.truncate()
    db.commit()
    redirect(URL('default', 'index'))