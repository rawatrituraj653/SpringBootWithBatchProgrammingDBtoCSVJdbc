package com.st.runner;

import java.util.Date;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class AppRunnner implements ApplicationRunner{

	@Autowired
	private JobLauncher launcher;
	@Autowired
	private Job job;
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		System.out.println("Job Execution");
		launcher.run(job, new JobParametersBuilder().addLong("time", System.currentTimeMillis())
										.addDate("date", new Date())
										.toJobParameters()
				);
	}
}
