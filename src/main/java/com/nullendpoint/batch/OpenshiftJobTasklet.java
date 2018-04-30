package com.nullendpoint.batch;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.List;

public class OpenshiftJobTasklet implements Tasklet {

    private String projectName;
    private String kubeJobName;
    private String imageName;
    private String pvcClaimName;
    private List<EnvVar> envVars;
    private String[] command;

    Logger log = LoggerFactory.getLogger(this.getClass());

    public OpenshiftJobTasklet(String projectName, String kubeJobName, String imageName, String pvcClaimName, List<EnvVar> envVarList, String... command){
        this.projectName = projectName;
        this.kubeJobName = kubeJobName;
        this.imageName = imageName;
        this.pvcClaimName = pvcClaimName;
        this.envVars = envVarList;
        this.command = command;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Long jobId = chunkContext.getStepContext().getStepExecution().getJobExecutionId();
        String jobName = this.kubeJobName + "-" + jobId.toString();

        OpenShiftClient osClient = new DefaultOpenShiftClient();
        final boolean[] jobComplete = {false};
        final boolean[] jobSuccess = {false};

        try (Watch watch = osClient.extensions().jobs().inNamespace(projectName).withName(jobName).watch(new Watcher<Job>() {
            @Override
            public void eventReceived(Action action, Job job) {
                if(action.name().equalsIgnoreCase("MODIFIED")){
                    if(job.getStatus().getSucceeded() != null | job.getStatus().getFailed() != null){
                        jobComplete[0] = true;
                        if(job.getStatus().getFailed() != null){
                            jobSuccess[0] = false;
                        }
                        if(job.getStatus().getSucceeded() != null){
                            jobSuccess[0] = true;
                        }
                    }
                }
            }

            @Override
            public void onClose(KubernetesClientException e) {
                if(e != null){
                    log.error("Problems with the Watcher", e);
                }else{
                    log.info("Completed Openshift container job: " + jobName);
                }

            }
        })){

            Job aJob = null;

            if(envVars != null){
                aJob = new JobBuilder()
                        .withNewMetadata().withName(jobName).addToLabels("job-name", jobName).endMetadata()
                        .withNewSpec()
                        .withNewTemplate()
                        .withNewMetadata().addToLabels("job-name", jobName).endMetadata()
                        .withNewSpec()
                        .withRestartPolicy("Never")
                        .addNewContainer().withName(jobName).withImage(imageName)
                        .withCommand(command)
                        .withVolumeMounts()
                        .addNewVolumeMount()
                        .withName("test")
                        .withMountPath("/test")
                        .endVolumeMount()
                        .withEnv(envVars)
                        .endContainer()
                        .addNewVolume()
                        .withName("test")
                        .withNewPersistentVolumeClaim(pvcClaimName, false)
                        .endVolume()
                        .endSpec()
                        .endTemplate()
                        .endSpec().build();
            }else{
                aJob = new JobBuilder()
                        .withNewMetadata().withName(jobName).addToLabels("job-name", jobName).endMetadata()
                        .withNewSpec()
                        .withNewTemplate()
                        .withNewMetadata().addToLabels("job-name", jobName).endMetadata()
                        .withNewSpec()
                        .withRestartPolicy("Never")
                        .addNewContainer().withName(jobName).withImage(imageName)
                        .withCommand(command)
                        .withVolumeMounts()
                        .addNewVolumeMount()
                        .withName("test")
                        .withMountPath("/test")
                        .endVolumeMount()
                        .endContainer()
                        .addNewVolume()
                        .withName("test")
                        .withNewPersistentVolumeClaim(pvcClaimName, false)
                        .endVolume()
                        .endSpec()
                        .endTemplate()
                        .endSpec().build();
            }

            osClient.extensions().jobs().inNamespace(projectName).create(aJob);

            while(!jobComplete[0]){
                Thread.sleep(5000);
            }
        }

        if(jobSuccess[0] == true){
            stepContribution.setExitStatus(ExitStatus.COMPLETED);
            return RepeatStatus.FINISHED;
        }else{
            stepContribution.setExitStatus(ExitStatus.FAILED);
            return RepeatStatus.FINISHED;
        }
    }
}
