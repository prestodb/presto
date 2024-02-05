pipeline {

    agent none

    environment {
        AWS_CREDENTIAL_ID  = 'aws-jenkins'
        AWS_DEFAULT_REGION = 'us-east-1'
        AWS_ECR            = 'public.ecr.aws/oss-presto'
        AWS_S3_PREFIX      = 's3://oss-prestodb/presto'
        IMG_NAME           = 'presto'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '100'))
        disableConcurrentBuilds()
        disableResume()
        overrideIndexTriggers(false)
        timeout(time: 3, unit: 'HOURS')
        timestamps()
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

            stages {
                stage('Setup') {
                    steps {
                        sh 'apt update && apt install -y awscli git tree'
                        sh 'git config --global --add safe.directory ${WORKSPACE}'
                        script {
                            env.PRESTO_COMMIT_SHA = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
                        }
                        echo "${PRESTO_COMMIT_SHA}"
                    }
                }

                stage('PR Update') {
                    when { changeRequest() }
                    steps {
                        echo 'get PR head commit sha'
                        sh 'git config --global --add safe.directory ${WORKSPACE}/presto-pr-${CHANGE_ID}'
                        script {
                            checkout $class: 'GitSCM',
                                    branches: [[name: 'FETCH_HEAD']],
                                    doGenerateSubmoduleConfigurations: false,
                                    extensions: [
                                        [
                                            $class: 'RelativeTargetDirectory',
                                            relativeTargetDir: "presto-pr-${env.CHANGE_ID}"
                                        ], [
                                            $class: 'CloneOption',
                                            shallow: true,
                                            noTags:  true,
                                            depth:   1,
                                            timeout: 100
                                        ], [
                                            $class: 'LocalBranch'
                                        ]
                                    ],
                                    submoduleCfg: [],
                                    userRemoteConfigs: [[
                                        refspec: "+refs/pull/${env.CHANGE_ID}/head:refs/remotes/origin/PR-${env.CHANGE_ID}",
                                        url: 'https://github.com/prestodb/presto'
                                    ]]
                            env.PRESTO_COMMIT_SHA = sh(script: "cd presto-pr-${env.CHANGE_ID} && git rev-parse HEAD", returnStdout: true).trim()
                        }
                        echo "${PRESTO_COMMIT_SHA}"
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
                                env.PRESTO_COMMIT_SHA.substring(0, 7)
                            env.DOCKER_IMAGE = env.AWS_ECR + "/${IMG_NAME}:${PRESTO_BUILD_VERSION}"
                        }
                        sh 'printenv | sort'

                        echo "build prestodb source code with build version ${PRESTO_BUILD_VERSION}"
                        retry (5) {
                            sh '''
                                unset MAVEN_CONFIG && ./mvnw install -DskipTests -B -T C1 -P ci -pl '!presto-docs'
                                tree /root/.m2/repository/com/facebook/presto/
                            '''
                        }

                        echo 'Publish Maven tarball'
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''
                                echo "${PRESTO_BUILD_VERSION}" > index.txt
                                git log -n 10 >> index.txt
                                aws s3 cp index.txt ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
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
                stage('Docker') {
                    steps {
                        echo 'build docker image'
                        sh 'apk update && apk add aws-cli bash git'
                        sh '''
                            docker run --privileged --rm tonistiigi/binfmt --install all
                            docker context ls
                            docker buildx create --name="container" --driver=docker-container --bootstrap
                            docker buildx ls
                            docker buildx inspect container
                        '''
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
                                REG_ORG=${AWS_ECR} IMAGE_NAME=${IMG_NAME} TAG=${PRESTO_BUILD_VERSION} ./build.sh ${PRESTO_VERSION}
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
                                cd docker/
                                aws s3 ls ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/
                                aws ecr-public get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                PUBLISH=true REG_ORG=${AWS_ECR} IMAGE_NAME=${IMG_NAME} TAG=${PRESTO_BUILD_VERSION} ./build.sh ${PRESTO_VERSION}
                            '''
                        }
                    }
                }
            }
        }
    }
}
