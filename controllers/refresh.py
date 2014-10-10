__author__ = 'dex'
def index():
    get_sample_tag_cross_tab()
    redirect(URL('default', 'index'))