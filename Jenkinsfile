pipeline {

    agent { label 'small' }

    environment {
        imagename = 'ghcr.io/pilotdataplatform/pipelinewatch'
        commit = sh(returnStdout: true, script: 'git describe --always').trim()
        registryCredential = 'pilot-ghcr'
    }

    stages {

        stage('DEV: Git clone') {
            when { branch 'develop' }
            steps {
                git branch: 'develop',
                    url: 'https://github.com/PilotDataPlatform/pipelinewatch.git',
                    credentialsId: 'pilot-gh'
            }
        }

        stage('DEV: Run unit tests') {
            when { branch 'develop' }
            steps {
                withCredentials([
                    string(credentialsId:'VAULT_TOKEN', variable: 'VAULT_TOKEN'),
                    string(credentialsId:'VAULT_URL', variable: 'VAULT_URL'),
                    file(credentialsId:'VAULT_CRT', variable: 'VAULT_CRT')
                ]) {
                    sh """
                    pip install --user poetry==1.1.12
                    ${HOME}/.local/bin/poetry config virtualenvs.in-project true
                    ${HOME}/.local/bin/poetry install --no-root --no-interaction
                    ${HOME}/.local/bin/poetry run pytest --verbose
                    """
                }
            }
        }

        stage('DEV: Build and push image') {
            when { branch 'develop' }
            steps {
                script {
                    docker.withRegistry('https://ghcr.io', registryCredential) {
                        customImage = docker.build('$imagename:$commit-CAC')
                        customImage.push()
                    }
                }
            }
        }

        stage('DEV: Remove image') {
            when { branch 'develop' }
            steps {
                sh 'docker rmi $imagename:$commit-CAC'
            }
        }

        stage('DEV: Deploy') {
            when { branch 'develop' }
            steps {
                build(job: '/VRE-IaC/UpdateAppVersion', parameters: [
                    [$class: 'StringParameterValue', name: 'TF_TARGET_ENV', value: 'dev'],
                    [$class: 'StringParameterValue', name: 'TARGET_RELEASE', value: 'pipelinewatch'],
                    [$class: 'StringParameterValue', name: 'NEW_APP_VERSION', value: "$commit-CAC"]
                ])
            }
        }
    }
}
