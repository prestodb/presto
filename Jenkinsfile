pipeline {

    agent none

    environment {
        AWS_CREDENTIAL_ID  = 'aws-jenkins'
        AWS_DEFAULT_REGION = 'us-east-1'
        AWS_ECR            = credentials('aws-ecr-private-registry')
        AWS_S3_PREFIX      = 's3://oss-jenkins/artifact/presto'
    }

    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '500'))
        timeout(time: 2, unit: 'HOURS')
    }

    parameters {
        booleanParam(name: 'PUBLISH_ARTIFACTS_ON_CURRENT_BRANCH',
                     defaultValue: false,
                     description: 'whether to publish tar and docker image even if current branch is not master'
        )
        booleanParam(name: 'BUILD_NATIVE_BASE_IMAGES',
                     defaultValue: false,
                     description: 'whether to build and publish native builder and runtime base images'
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

            stages {
                stage('Setup') {
                    steps {
                        sh 'apt update && apt install -y awscli git tree'
                        sh 'git config --global --add safe.directory ${WORKSPACE}'
                    }
                }

                stage('Maven') {
                    steps {
                        sh 'unset MAVEN_CONFIG && ./mvnw versions:set -DremoveSnapshot'
                        script {
                            env.PRESTO_VERSION = sh(
                                script: 'unset MAVEN_CONFIG && ./mvnw org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout',
                                returnStdout: true).trim()
                            env.PRESTO_PKG = "presto-server-${PRESTO_VERSION}.tar.gz"
                            env.PRESTO_CLI_JAR = "presto-cli-${PRESTO_VERSION}-executable.jar"
                            env.PRESTO_BUILD_VERSION = env.PRESTO_VERSION + '-' +
                                sh(script: "git show -s --format=%cd --date=format:'%Y%m%d%H%M%S'", returnStdout: true).trim() + "-" +
                                env.GIT_COMMIT.substring(0, 7)
                            env.DOCKER_IMAGE = env.AWS_ECR + "/oss-presto/presto:${PRESTO_BUILD_VERSION}"
                            env.DOCKER_NATIVE_IMAGE = env.AWS_ECR + "/oss-presto/presto-native:${PRESTO_BUILD_VERSION}"
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
                stage('Setup') {
                    steps {
                        echo 'build docker image'
                        sh 'apk update && apk add aws-cli bash git make'
                        sh 'git config --global --add safe.directory ${WORKSPACE}'
                        sh '''#!/bin/bash -ex
                            cd presto-native-execution/
                            git config --global --add safe.directory ${WORKSPACE}/presto-native-execution/velox
                            make velox-submodule
                        '''
                    }
                }

                stage('Docker') {
                    steps {
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
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

                stage('Native Builder Image') {
                    when {
                        expression { params.BUILD_NATIVE_BASE_IMAGES }
                    }
                    steps {
                        script {
                            env.NATIVE_BUILDER_IMAGE = env.AWS_ECR + "/oss-presto/presto-native-builder:${PRESTO_BUILD_VERSION}"
                            env.NATIVE_BUILDER_IMAGE_LATEST = env.AWS_ECR + "/oss-presto/presto-native-builder:latest"
                        }
                        echo "Building ${NATIVE_BUILDER_IMAGE}"
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                cd presto-native-execution/
                                docker buildx build -f Dockerfile.0.buildtime --load --platform "linux/amd64" \
                                    -t "${NATIVE_BUILDER_IMAGE}-amd64" \
                                    -t "${NATIVE_BUILDER_IMAGE_LATEST}-amd64" \
                                    .
                                aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                docker push "${NATIVE_BUILDER_IMAGE_LATEST}-amd64"
                                docker push "${NATIVE_BUILDER_IMAGE}-amd64"
                            '''
                        }
                    }
                }

                stage('Native Runtime Image') {
                    when {
                        expression { params.BUILD_NATIVE_BASE_IMAGES }
                    }
                    steps {
                        script {
                            env.NATIVE_RUNTIME_IMAGE = env.AWS_ECR + "/oss-presto/presto-native-runtime:${PRESTO_BUILD_VERSION}"
                            env.NATIVE_RUNTIME_IMAGE_LATEST = env.AWS_ECR + "/oss-presto/presto-native-runtime:latest"
                        }
                        echo "Building ${NATIVE_RUNTIME_IMAGE}"
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                cd presto-native-execution/
                                docker buildx build -f Dockerfile.0.runtime --load --platform "linux/amd64" \
                                    -t "${NATIVE_RUNTIME_IMAGE}-amd64" \
                                    -t "${NATIVE_RUNTIME_IMAGE_LATEST}-amd64" \
                                    .
                                aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                docker push "${NATIVE_RUNTIME_IMAGE_LATEST}-amd64"
                                docker push "${NATIVE_RUNTIME_IMAGE}-amd64"
                            '''
                        }
                    }
                }

                stage('Docker Native') {
                    steps {
                        echo "Building ${DOCKER_NATIVE_IMAGE}"
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                cd presto-native-execution/
                                aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                docker buildx build -f Dockerfile.1.prestissimo --load --platform "linux/amd64" \
                                    -t "${DOCKER_NATIVE_IMAGE}-amd64" \
                                    --build-arg "IMAGE_REGISTRY=${AWS_ECR}" \
                                    --build-arg "BUILDTIME_IMAGE=oss-presto/presto-native-builder" \
                                    --build-arg "RUNTIME_IMAGE=oss-presto/presto-native-runtime" \
                                    --build-arg "IMAGE_TAG=latest-amd64" \
                                    .
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
                                docker push "${DOCKER_NATIVE_IMAGE}-amd64"
                            '''
                        }
                    }
                }
            }
        }
    }
}