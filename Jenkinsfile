// ⚠ THIS FILE IS NOT WHAT RUNS. Editing it changes nothing.
//
// The live job "DSLF-Email-Scanner" is a FREESTYLE "Execute shell" step whose script is
// stored in the Jenkins job config, not here. Its build log opens with
// `/bin/sh -xe /tmp/jenkins….sh` and it does `rm -rf email_to_jira && git clone …` into a
// subdirectory, builds a venv, copies a credential file to .env, then runs:
//
//     python email_scanner/email_scanner.py
//     python qc_checker.py
//
// On 2026-08-31 that last line broke the build, because `qc_checker.py` had been deleted
// and this Jenkinsfile — updated in the same commit to call `qc_llm.py` — is inert.
// `qc_checker.py` is now a shim that forwards to qc_llm; see its docstring.
//
// If this ever becomes the live config, note the shim exists for a reason: qc_llm.main()
// returns 1 on any FAIL/UNVERIFIED, and under `sh -xe` that reds the build every time a
// ticket legitimately fails QC. Call qc_checker.py, or replicate its exit-code handling.
pipeline {
    agent any

    options {
        timeout(time: 15, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '100'))
        disableConcurrentBuilds()
    }

    triggers {
        cron('H/5 * * * *')
    }

    environment {
        JIRA_BASE_URL     = 'https://rkdgroup.atlassian.net'
        JIRA_EMAIL        = credentials('DSLF_JIRA_EMAIL')
        JIRA_API_TOKEN    = credentials('DSLF_JIRA_API_TOKEN')
        ANTHROPIC_API_KEY = credentials('DSLF_ANTHROPIC_API_KEY')
        MS_CLIENT_ID      = credentials('DSLF_MS_CLIENT_ID')
        MS_TENANT_ID      = credentials('DSLF_MS_TENANT_ID')
        IMAP_EMAIL        = credentials('DSLF_IMAP_EMAIL')
        IBMI_HOST         = credentials('DSLF_IBMI_HOST')
        IBMI_USER         = credentials('DSLF_IBMI_USER')
        IBMI_PASSWORD     = credentials('DSLF_IBMI_PASSWORD')
        IBMI_JT400_JAR    = "${WORKSPACE}/jt400.jar"
        // Caps the QC stage so it cannot consume the whole build. Tickets
        // past it come back UNVERIFIED and are re-checked next run.
        QC_BUDGET_S       = '420'
    }

    stages {
        stage('Install deps') {
            steps {
                sh 'pip3 install -q -r requirements.txt'
            }
        }
        stage('Scan emails') {
            steps {
                sh 'python3 email_scanner/email_scanner.py'
            }
        }
        stage('Scan QC queue') {
            steps {
                // Via the shim, for its exit-code handling — see the header.
                sh 'python3 qc_checker.py'
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'email_scanner/logs/*.log',
                             allowEmptyArchive: true
        }
        failure {
            echo 'Scan failed — check archived logs above.'
        }
    }
}
