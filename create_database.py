from init import do_init
from catalogs import do_catalogs
from catalogs.flamsteed import do_flamsteed
from catalogs.gliese import do_gliese
from catalogs.hd import do_hd

if __name__ == "__main__":
    do_init()
    do_catalogs()
    do_flamsteed()
    # do_gliese()
    # do_hd()