node {
    stage('Checkout') {
      checkout scm
    }

    stage('Build') {
      build = docker.image("gradle").inside("--network=host") {

        stage('Build') {
          sh "gradle clean build"
        }

        stage('Test') {
          sh "gradle test"
          junit 'build/test-results/test/*.xml'
        }

        if (env.BRANCH_NAME == 'master'  || env.BRANCH_NAME == 'dev') {
            stage('Deploy') {
              sh "gradle publish"
            }
        }
      }
    }
}
