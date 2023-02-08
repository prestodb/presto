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
        timeout(time: 3, unit: 'HOURS')
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
                sh '''
                    apt update && apt install -y awscli git tree
                    git config --global --add safe.directory "${WORKSPACE}"
                '''
                sh 'unset MAVEN_CONFIG && ./mvnw versions:set -DremoveSnapshot'

                script {
                    env.PRESTO_VERSION = sh(
                        script: 'unset MAVEN_CONFIG && ./mvnw org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout',
                        returnStdout: true).trim()
                    env.PRESTO_PKG = "presto-server-${PRESTO_VERSION}.tar.gz"
                    env.PRESTO_CLI_JAR = "presto-cli-${PRESTO_VERSION}-executable.jar"
                    env.PRESTO_BUILD_VERSION = env.PRESTO_VERSION + '-' +
                        sh(script: "git show -s --format=%cd --date=format:'%Y%m%d%H%M%S'", returnStdout: true).trim() + "-" +
                        sh(script: "git rev-parse --short=7 HEAD", returnStdout: true).trim()
                    env.DOCKER_IMAGE = env.AWS_ECR + "/oss-presto/presto:${PRESTO_BUILD_VERSION}"
                    env.DOCKER_NATIVE_IMAGE = env.AWS_ECR + "/oss-presto/presto-native:${PRESTO_BUILD_VERSION}"
                }
                sh 'printenv | sort'

                echo "build prestodb source code with build version ${PRESTO_BUILD_VERSION}"
                sh '''
                    exit 0
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
                        exit 0
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
                stage('Java Image') {
                    steps {
                        echo 'build docker image'
                        sh '''
                            apk update && apk add aws-cli bash git make
                        '''
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                exit 0
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

                stage('Native Image') {
                    steps {
                        echo "Building ${DOCKER_NATIVE_IMAGE}"
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                cd presto-native-execution/scripts/
                                ./build-centos.sh
                                docker image ls
                                # docker tag presto/prestissimo-avx-centos:latest "${DOCKER_NATIVE_IMAGE}-amd64"
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
                                docker push "${DOCKER_NATIVE_IMAGE}-amd64"
                            '''
                        }
                    }
                }
            }
        }
    }
}
