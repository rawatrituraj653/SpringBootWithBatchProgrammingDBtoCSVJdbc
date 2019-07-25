package com.st.config;


import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.st.listener.JobEXListener;
import com.st.model.Travel;

@Configuration
@EnableBatchProcessing
@EnableTransactionManagement
public class BatchConfig {

	
	@Autowired
	private JobBuilderFactory jf;
	@Autowired
	private StepBuilderFactory sf;
	
	@Bean
	public Step step() {
		return sf.get("step")
				.<Travel,Travel>chunk(4)
				.reader(read())
				.processor(process())
				.writer(write())
				.build();
	}
	@Bean
	public Job job() {
	
		return jf.get("job")
				.incrementer(new RunIdIncrementer())
				.listener(listener())
				.start(step())
				.build();
	}
	
	
	
	@Bean
	public ItemReader<Travel> read(){
		System.out.println("item Readerss");
		JdbcCursorItemReader<Travel> jdbc=new JdbcCursorItemReader<>();
			jdbc.setDataSource(ds());
			jdbc.setSql("select * from travelagency");
			jdbc.setRowMapper(
					new RowMapper<Travel>() {
						@Override
						public Travel mapRow(ResultSet rs, int rowNum) throws SQLException {
							Travel t=new Travel();
							t.setFlightId(rs.getString("flightId"));
							t.setFlightName(rs.getString("flightName"));
							t.setPilotName(rs.getString("pilotName"));
							t.setAgentId(rs.getString("agentId"));
							t.setTicketCost(rs.getDouble("ticketCost"));
							t.setDiscount(rs.getDouble("discount"));
							t.setGst(rs.getDouble("gst"));
							t.setFinalAmount(rs.getDouble("finalAmount"));
							return t;
						}});
		
		return jdbc;
	}
	
	@Bean
	public ItemProcessor<Travel,Travel> process(){
		return travel->{
		
			System.out.println(travel);
			return travel;
		};
	}
	@Bean
	public ItemWriter<Travel> write(){
		System.out.println("item Writer");
		FlatFileItemWriter<Travel> writer=new FlatFileItemWriter<>();
		writer.setResource(new ClassPathResource("Mydata.csv"));
		writer.setAppendAllowed(true);
		writer.setLineAggregator(new  DelimitedLineAggregator<Travel>() {{
			setDelimiter(",");
		setFieldExtractor(new BeanWrapperFieldExtractor<Travel>() {{
			setNames(new String[]{"flightId","flightName","pilotName","agentId","ticketCost","discount","gst","finalAmount"} );
			}});
		}});
		return writer;
	}
	
	

	@Bean
	public DataSource ds() {
		DriverManagerDataSource dataSource=new DriverManagerDataSource();
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://localhost:3306/batch");
		dataSource.setUsername("root");
		dataSource.setPassword("root");
		
		return dataSource;
	}
	
	@Bean
	public JobExecutionListener listener() {
		
		return new JobEXListener();
	}
	
}
