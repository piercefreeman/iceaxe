from setuptools import setup, Extension
from Cython.Build import cythonize

setup(
    name="iceaxe",
    ext_modules=cythonize(
        [
            Extension("iceaxe.session_optimized", ["iceaxe/session_optimized.pyx"]),
        ],
        compiler_directives={'language_level': "3"}
    )
)
