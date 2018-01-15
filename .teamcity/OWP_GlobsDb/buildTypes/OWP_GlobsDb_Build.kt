package OWP_GlobsDb.buildTypes

import jetbrains.buildServer.configs.kotlin.v2017_2.*
import jetbrains.buildServer.configs.kotlin.v2017_2.buildSteps.gradle
import jetbrains.buildServer.configs.kotlin.v2017_2.triggers.vcs
import jetbrains.buildServer.configs.kotlin.v2017_2.buildFeatures.commitStatusPublisher

object OWP_GlobsDb_Build : BuildType({
    uuid = "33f9b299-dff2-4339-8ad9-317c6c3e2c7e"
    id = "OWP_GlobsDb_Build"
    name = "Build"

    vcs {
        root(OWP_GlobsDb.vcsRoots.OWP_GlobsDb_Repository_Dev)

    }

    steps {
        gradle {
            tasks = "buildDependents"
            incremental = true
            useGradleWrapper = true
            enableStacktrace = true
            coverageEngine = idea {
                includeClasses = "org.globsframework.*"
            }
            dockerImage = "gradle:4.3.1"
            dockerRunParameters = "--net=host"
        }
        gradle {
            tasks = "publish"
            useGradleWrapper = true
            enableStacktrace = true
            dockerImage = "gradle:4.3.1"
            dockerRunParameters = "--net=host"
        }
    }

    features {
        commitStatusPublisher {
            publisher = github {
                githubUrl = "https://api.github.com"
                authType = personalToken {
                    token = "credentialsJSON:5bc09a01-94ad-44aa-a6d6-3e1fcc90d6c2"
                }
            }
        }
    }

    triggers {
        vcs {
        }
    }

    dependencies {
        dependency("OWP_Globsframework_Build") {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }
        }
    }
})
