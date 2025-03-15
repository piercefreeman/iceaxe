from setuptools import setup
from Cython.Build import cythonize

setup(
    name="iceaxe",
    ext_modules=cythonize("iceaxe/**/*.pyx", annotate=True, language_level="3"),
)
