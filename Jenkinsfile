pipeline {

    agent none

    environment {
        AWS_CREDENTIAL_ID  = 'aws-jenkins'
        AWS_DEFAULT_REGION = 'us-east-1'
        AWS_ECR            = credentials('aws-ecr-private-registry')
        AWS_S3_PREFIX      = 's3://oss-jenkins/artifact/presto'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '500'))
        timeout(time: 2, unit: 'HOURS')
    }

    parameters {
        booleanParam(name: 'PUBLISH_ARTIFACTS_ON_CURRENT_BRANCH',
                     defaultValue: false,
                     description: 'whether to publish tar and docker image even if current branch is not master'
        )
    }

    stages {
        stage('Maven Build') {
            agent {
                kubernetes {
                    defaultContainer 'maven'
                    yamlFile 'jenkins/agent-maven.yaml'
                }
            }

            steps {
                sh 'apt update && apt install -y awscli git tree'
                sh 'unset MAVEN_CONFIG && ./mvnw versions:set -DremoveSnapshot'

                script {
                    env.PRESTO_VERSION = sh(
                        script: 'unset MAVEN_CONFIG && ./mvnw org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout',
                        returnStdout: true).trim()
                    env.PRESTO_PKG = "presto-server-${PRESTO_VERSION}.tar.gz"
                    env.PRESTO_CLI_JAR = "presto-cli-${PRESTO_VERSION}-executable.jar"
                    env.PRESTO_BUILD_VERSION = env.PRESTO_VERSION + '-' +
                                            sh(script: 'TZ=UTC date +%Y%m%dT%H%M%S', returnStdout: true).trim() + '-' +
                                            sh(script: 'git rev-parse --short=7 HEAD', returnStdout: true).trim()
                    env.DOCKER_IMAGE = env.AWS_ECR + "/oss-presto/presto:${PRESTO_BUILD_VERSION}"
                }
                sh 'printenv | sort'

                echo "build prestodb source code with build version ${PRESTO_BUILD_VERSION}"
                sh '''
                    unset MAVEN_CONFIG && ./mvnw install -DskipTests -B -T C1 -P ci -pl '!presto-docs'
                    tree /root/.m2/repository/com/facebook/presto/
                '''

                echo 'Publish Maven tarball'
                withCredentials([[
                        $class:            'AmazonWebServicesCredentialsBinding',
                        credentialsId:     "${AWS_CREDENTIAL_ID}",
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                    sh '''
                        aws s3 cp presto-server/target/${PRESTO_PKG}  ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
                        aws s3 cp presto-cli/target/${PRESTO_CLI_JAR} ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
                    '''
                }
            }
        }

        stage('Docker Build') {
            agent {
                kubernetes {
                    defaultContainer 'dind'
                    yamlFile 'jenkins/agent-dind.yaml'
                }
            }

            stages {
                stage('Docker') {
                    steps {
                        echo 'build docker image'
                        sh 'apk update && apk add aws-cli bash git'
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                for dir in /home/jenkins/agent/workspace/*/; do
                                    echo "${dir}"
                                    git config --global --add safe.directory "${dir:0:-1}"
                                done

                                cd docker/
                                aws s3 cp ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/${PRESTO_PKG}     . --no-progress
                                aws s3 cp ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/${PRESTO_CLI_JAR} . --no-progress

                                echo "Building ${DOCKER_IMAGE}"
                                docker buildx build --load --platform "linux/amd64" -t "${DOCKER_IMAGE}-amd64" \
                                    --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" .
                            '''
                        }
                    }
                }

                stage('Publish Docker') {
                    when {
                        anyOf {
                            expression { params.PUBLISH_ARTIFACTS_ON_CURRENT_BRANCH }
                            branch "master"
                        }
                        beforeAgent true
                    }

                    steps {
                        echo 'Publish docker image'
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''
                                aws s3 ls ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/
                                docker image ls
                                aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                docker push "${DOCKER_IMAGE}-amd64"
                            '''
                        }
                    }
                }
            }
        }
    }
}
