import platform

from setuptools import Extension, setup


def get_ext_modules() -> list:
    """
    获取三方模块

    Linux需要编译封装接口
    Windows直接使用预编译的pyd即可
    Mac由于缺乏二进制库支持无法使用
    """
    if platform.system() != "Linux":
        return []

    compiler_flags = [
        "-std=c++17",
        "-O3",
        "-Wno-delete-incomplete", "-Wno-sign-compare",
    ]
    extra_link_args = ["-lstdc++"]
    runtime_library_dirs = ["$ORIGIN"]

    vnnhmd = Extension(
        "vnpy_nhtd.api.vnnhmd",
        [
            "vnpy_nhtd/api/vnnh/vnnhmd/vnnhmd.cpp",
        ],
        include_dirs=["vnpy_nhtd/api/include",
                      "vnpy_nhtd/api/vnnh"],
        define_macros=[],
        undef_macros=[],
        library_dirs=["vnpy_nhtd/api/libs", "vnpy_nhtd/api"],
        libraries=["nhmdapi", "nhtd2traderapi", "nhtd2traderapi_rdma", "nhtd2traderapi_tcpdirect", "nhtdstockapi"],
        extra_compile_args=compiler_flags,
        extra_link_args=extra_link_args,
        runtime_library_dirs=runtime_library_dirs,
        depends=[],
        language="cpp",
    )

    vnnhfutures = Extension(
        "vnpy_nhtd.api.vnnhfutures",
        [
            "vnpy_nhtd/api/vnnh/vnnhfutures/vnnhfutures.cpp",
        ],
        include_dirs=["vnpy_nhtd/api/include",
                      "vnpy_nhtd/api/vnnh"],
        define_macros=[],
        undef_macros=[],
        library_dirs=["vnpy_nhtd/api/libs", "vnpy_nhtd/api"],
        libraries=["nhmdapi", "nhtd2traderapi", "nhtd2traderapi_rdma", "nhtd2traderapi_tcpdirect", "nhtdstockapi"],
        extra_compile_args=compiler_flags,
        extra_link_args=extra_link_args,
        runtime_library_dirs=runtime_library_dirs,
        depends=[],
        language="cpp",
    )

    vnnhstock = Extension(
        "vnpy_nhtd.api.vnnhstock",
        [
            "vnpy_nhtd/api/vnnh/vnnhstock/vnnhstock.cpp",
        ],
        include_dirs=["vnpy_nhtd/api/include",
                      "vnpy_nhtd/api/vnnh"],
        define_macros=[],
        undef_macros=[],
        library_dirs=["vnpy_nhtd/api/libs", "vnpy_nhtd/api"],
        libraries=["nhmdapi", "nhtd2traderapi", "nhtd2traderapi_rdma", "nhtd2traderapi_tcpdirect", "nhtdstockapi"],
        extra_compile_args=compiler_flags,
        extra_link_args=extra_link_args,
        runtime_library_dirs=runtime_library_dirs,
        depends=[],
        language="cpp",
    )

    return [vnnhmd, vnnhfutures, vnnhstock]


setup(
    ext_modules=get_ext_modules(),
)
