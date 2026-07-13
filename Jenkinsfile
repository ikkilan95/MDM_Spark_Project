pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'pipenv --python python3 sync'
            }
        }

        stage('Test') {
            steps {
                sh 'pipenv run pytest'
            }
        }

        stage('Package') {
            steps {
                // This packages your local files into a zip archive on your machine
                sh 'zip -r sbdl.zip lib'
            }
        }

        stage('Local Simulate Deploy') {
            steps {
                // Simulates a deployment locally by printing a success message
                echo "Build completed successfully! Package 'sbdl.zip' is ready in the workspace."
            }
        }
    }
}