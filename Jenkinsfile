pipeline {

    agent {
        kubernetes {
            defaultContainer 'maven'
            yamlFile 'jenkins/agent.yaml'
        }
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '100'))
        timeout(time: 2, unit: 'HOURS')
    }

    stages {
        stage ('Test') {
            steps {
                echo "test"
            }
        }
    }

    post { 
        fixed {
            slackSend(color: "good", message: "fixed ${RUN_DISPLAY_URL}")
        }
        failure {
            slackSend(color: "danger", message: "failure ${RUN_DISPLAY_URL}")
        }
    }
}
