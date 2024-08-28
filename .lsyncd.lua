settings {
    nodaemon = true,
}

sync {
    default.rsyncssh,
    source = ".",
    host = "marx",
    targetdir = "/data/tlouf/code/data-collectors",
    excludeFrom = ".gitignore",
    exclude = { ".git" },
    delete = false,
    delay = 1,
}
