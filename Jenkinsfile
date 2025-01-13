pipeline {
    agent {
        docker {
            image 'rustlang/rust:nightly'
        }
    }
    
    stages {
        stage('Build') {
            steps {
                checkout scm
                sh 'cargo +nightly build'
            }
        }
        stage('Test') {
            steps {
                ansiColor('xterm') {  
                    sh 'cargo +nightly test'
                }
            }
        }
        stage('Doc') {
            steps {
                sh 'cargo doc'
                step([$class: 'JavadocArchiver',
                      javadocDir: 'target/doc',
                      keepAll: false])
            }
        }
    }

    post {
        always {
            deleteDir()
        }
    }
}