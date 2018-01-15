package OWP_GlobsDb

import OWP_GlobsDb.buildTypes.*
import OWP_GlobsDb.vcsRoots.*
import OWP_GlobsDb.vcsRoots.OWP_GlobsDb_Repository_Dev
import jetbrains.buildServer.configs.kotlin.v2017_2.*
import jetbrains.buildServer.configs.kotlin.v2017_2.Project
import jetbrains.buildServer.configs.kotlin.v2017_2.projectFeatures.VersionedSettings
import jetbrains.buildServer.configs.kotlin.v2017_2.projectFeatures.versionedSettings

object Project : Project({
    uuid = "56c0822b-8153-4f41-9c56-80cd1a638218"
    id = "OWP_GlobsDb"
    parentId = "OWP"
    name = "Globs DB"

    vcsRoot(OWP_GlobsDb_Repository_Dev)

    buildType(OWP_GlobsDb_Build)

    features {
        versionedSettings {
            id = "PROJECT_EXT_12"
            mode = VersionedSettings.Mode.ENABLED
            buildSettingsMode = VersionedSettings.BuildSettingsMode.USE_CURRENT_SETTINGS
            rootExtId = OWP_GlobsDb_Repository_Dev.id
            showChanges = true
            settingsFormat = VersionedSettings.Format.KOTLIN
            storeSecureParamsOutsideOfVcs = true
        }
    }
})
