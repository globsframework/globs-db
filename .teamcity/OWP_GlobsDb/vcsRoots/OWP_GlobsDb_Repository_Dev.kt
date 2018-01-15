package OWP_GlobsDb.vcsRoots

import jetbrains.buildServer.configs.kotlin.v2017_2.*
import jetbrains.buildServer.configs.kotlin.v2017_2.vcs.GitVcsRoot

object OWP_GlobsDb_Repository_Dev : GitVcsRoot({
    uuid = "473752a9-927b-4177-aee4-aa017e6a4aed"
    id = "OWP_GlobsDb_Repository_Dev"
    name = "https://github.com/onewealthplace/globs-db#refs/heads/dev"
    url = "https://github.com/onewealthplace/globs-db"
    branch = "refs/heads/dev"
    authMethod = password {
        userName = "cedbossneo"
        password = "credentialsJSON:5bc09a01-94ad-44aa-a6d6-3e1fcc90d6c2"
    }
})
