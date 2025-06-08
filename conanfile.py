from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout


class MigrationServer(ConanFile):
    name = "migration_server"
    version = "1.0.0"

    # Binary configuration
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    # Sources are located in the same place as this recipe
    exports_sources = "CMakeLists.txt", "src/*", "include/*"

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        self.requires("libpqxx/7.10.1")
        self.requires("libpq/[>=14 <17]")
        self.requires("libcurl/8.5.0")
        self.requires("boost/1.87.0", override=True)
        self.requires("fmt/11.1.3")
        self.requires("spdlog/1.15.1")
        self.requires("msgpack-cxx/4.1.1")
        self.requires("abseil/20230802.1")
        self.requires("zlib/1.2.12")
        self.requires("otterbrix/1.0.0a10-rc-3")
        self.requires("magic_enum/0.8.1")
        self.requires("actor-zeta/1.0.0a12")


    def config_options(self):
        # Setting options for packages
        self.options["libpqxx/*"].shared = True
        self.options["libpq/*"].shared = True
        self.options["otterbrix/*"].shared = True

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def imports(self):
        # Import DLLs, dylibs and so files
        self.copy("*.dll", src="lib", dst="bin")  # Windows DLLs
        self.copy("*.dylib*", src="lib", dst="bin")  # macOS dylibs
        self.copy("*.so*", src="lib", dst="bin")  # Linux shared libraries