AGENT_MAVEN = '''
    apiVersion: v1
    kind: Pod
    spec:
        containers:
        - name: maven
          image: maven:3.8.6-openjdk-8-slim
          env:
          - name: MAVEN_OPTS
            value: "-Xmx8000m -Xms8000m"
          resources:
            requests:
              memory: "16Gi"
              cpu: "4000m"
            limits:
              memory: "16Gi"
              cpu: "4000m"
          tty: true
          command:
          - cat
'''

AGENT_DIND = '''
    apiVersion: v1
    kind: Pod
    spec:
        containers:
        - name: dind
          image: docker:20.10.16-dind-alpine3.15
          securityContext:
            privileged: true
          tty: true
          resources:
            requests:
              memory: "29Gi"
              cpu: "7500m"
            limits:
              memory: "29Gi"
              cpu: "7500m"
'''

pipeline {

    agent none

    environment {
        AWS_CREDENTIAL_ID  = 'oss-presto-aws'
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
                     description: 'whether to publish tar and docker image even if current branch is not master')
        booleanParam(name: 'BUILD_PRESTISSIMO_DEPENDENCY',
                     defaultValue: false,
                     description: 'Check to build a new native dependency image because of the build dependency updates')
    }

    stages {
        stage('Maven Build') {
            agent {
                kubernetes {
                    defaultContainer 'maven'
                    yaml AGENT_MAVEN
                }
            }

            stages {
                stage('Maven Agent Setup') {
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
                            env.PRESTO_FUNCTION_SERVER_JAR = "presto-function-server-${PRESTO_VERSION}-executable.jar"
                            env.PRESTO_BUILD_VERSION = env.PRESTO_VERSION + '-' +
                                sh(script: "git show -s --format=%cd --date=format:'%Y%m%d%H%M%S'", returnStdout: true).trim() + "-" +
                                env.PRESTO_COMMIT_SHA.substring(0, 7)
                            env.DOCKER_IMAGE = env.AWS_ECR + "/${IMG_NAME}:${PRESTO_BUILD_VERSION}"
                            env.NATIVE_DOCKER_IMAGE_DEPENDENCY = env.AWS_ECR + "/presto-native-dependency:${PRESTO_BUILD_VERSION}"
                            env.NATIVE_DOCKER_IMAGE = env.AWS_ECR + "/presto-native:${PRESTO_BUILD_VERSION}"
                        }
                        sh 'printenv | sort'

                        echo "build prestodb source code with build version ${PRESTO_BUILD_VERSION}"
                        retry (5) {
                            sh '''
                                unset MAVEN_CONFIG && ./mvnw install -DskipTests -B -T 1C -P ci -pl '!presto-docs'
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
                                aws s3 cp presto-server/target/${PRESTO_PKG}                            ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
                                aws s3 cp presto-cli/target/${PRESTO_CLI_JAR}                           ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
                                aws s3 cp presto-function-server/target/${PRESTO_FUNCTION_SERVER_JAR}   ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
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
                    yaml AGENT_DIND
                }
            }

            stages {
                stage('Docker Agent Setup') {
                    steps {
                        sh 'apk update && apk add aws-cli bash git make'
                        sh 'git config --global --add safe.directory ${WORKSPACE}'
                        sh 'git config --global --add safe.directory ${WORKSPACE}/presto-native-execution/velox'
                        sh '''
                            docker run --privileged --rm tonistiigi/binfmt --install all
                            docker context ls
                            docker buildx create --name="container" --driver=docker-container --bootstrap
                            docker buildx ls
                            docker buildx inspect container
                        '''
                    }
                }

                stage('Docker Java') {
                    steps {
                        echo 'build docker image'
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                cd docker/
                                aws s3 cp ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/${PRESTO_PKG}                    . --no-progress
                                aws s3 cp ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/${PRESTO_CLI_JAR}                . --no-progress
                                aws s3 cp ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/${PRESTO_FUNCTION_SERVER_JAR}    . --no-progress

                                echo "Building ${DOCKER_IMAGE}"
                                REG_ORG=${AWS_ECR} IMAGE_NAME=${IMG_NAME} TAG=${PRESTO_BUILD_VERSION} ./build.sh ${PRESTO_VERSION}
                            '''
                        }
                    }
                }

                stage('Publish Docker Java') {
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
                                cd docker/
                                aws s3 ls ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/
                                aws ecr-public get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                PUBLISH=true REG_ORG=${AWS_ECR} IMAGE_NAME=${IMG_NAME} TAG=${PRESTO_BUILD_VERSION} ./build.sh ${PRESTO_VERSION}
                            '''
                        }
                    }
                }

                stage ('Build Docker Native Dependency') {
                    when {
                        environment name: 'BUILD_PRESTISSIMO_DEPENDENCY', value: 'true'
                    }
                    steps {
                        sh '''
                            cd presto-native-execution/
                            make submodules
                            docker buildx build --load --platform "linux/amd64" \
                                    -t "${NATIVE_DOCKER_IMAGE_DEPENDENCY}" \
                                    -f scripts/dockerfiles/centos-dependency.dockerfile \
                                    .
                            docker tag "${NATIVE_DOCKER_IMAGE_DEPENDENCY}" "${AWS_ECR}/presto-native-dependency:latest"
                            docker image ls
                        '''
                    }
                }

                stage ('Build Docker Native') {
                    steps {
                        sh '''
                            cd presto-native-execution/
                            make submodules
                            docker buildx build --load --platform "linux/amd64" \
                                    -t "${NATIVE_DOCKER_IMAGE}" \
                                    --build-arg BUILD_TYPE=Release \
                                    --build-arg DEPENDENCY_IMAGE=${AWS_ECR}/presto-native-dependency:latest \
                                    --build-arg "EXTRA_CMAKE_FLAGS=-DPRESTO_ENABLE_TESTING=OFF -DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_S3=ON" \
                                    -f scripts/dockerfiles/prestissimo-runtime.dockerfile \
                                    .
                            docker image ls
                        '''
                    }
                }

                stage('Publish Docker Native') {
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
                                aws ecr-public get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                if ${BUILD_PRESTISSIMO_DEPENDENCY}
                                then
                                    docker push "${NATIVE_DOCKER_IMAGE_DEPENDENCY}"
                                    docker push "${AWS_ECR}/presto-native-dependency:latest"
                                fi
                                docker push "${NATIVE_DOCKER_IMAGE}"
                            '''
                        }
                    }
                }
            }
        }
    }
}
