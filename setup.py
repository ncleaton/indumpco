from setuptools import setup, find_packages, Extension
setup(
    name = "InDumpCo",
    version = "0.1",
    packages = ['indumpco'],
	scripts = ['bin/indumpco-create', 'bin/indumpco-extract'],
    ext_modules = [Extension("indumpco.fletcher_sum_split", sources=["fletcher_sum_split.c"])],
	test_suite = 'nose.collector',

    author = "Nick Cleaton",
    author_email = "nick@cleaton.net",
    description = "Incremental Dump Compressor",
    license = "MIT",
)
