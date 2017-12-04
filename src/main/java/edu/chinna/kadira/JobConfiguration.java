package edu.chinna.kadira;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.bson.Document;
import org.json.JSONException;
import org.json.XML;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	public static int PRETTY_PRINT_INDENT_FACTOR = 4;

	public static String FILE_NAME = "BreakfastMenu.xml";

	public static String STEP_ONE = "step1";

	public static String STEP_TWO = "step2";

	public static String MONGO_DOC = "doc";

	public static String XML_PROCESSOR = "XML_Processor";

	public static String NEW_LINE = "\n";

	/***
	 * Batch Job
	 * 
	 * @return
	 */
	@Bean
	public Job xmlToJsonToMongo() {
		return jobBuilderFactory.get(XML_PROCESSOR).start(stepOne()).next(stepTwo()).build();
	}

	/***
	 * Step One
	 * 
	 * @return
	 */
	public Step stepOne() {
		return stepBuilderFactory.get(STEP_ONE).tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
				mongoTemplate.insert(Document.parse(processXML2JSON(Paths.get(getFilePath()))), MONGO_DOC);
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	/**
	 * Step Two
	 * 
	 * @return
	 */
	public Step stepTwo() {
		return stepBuilderFactory.get(STEP_TWO).tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

				String result = doCollect();
				String resultTwo = doCollectTwo();
				String resultThree = doCollectThree();

				System.out.println(" RESULT:::::::::::::::::::::" + result);
				System.out.println(" RESULT:::::::::::::::::::::" + resultTwo);
				System.out.println(" RESULT:::::::::::::::::::::" + resultThree);

				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	/***
	 * This will return the id of the breakfastmenu that has a specific style
	 * 
	 * @return
	 */
	public String doCollect() {
		Query query = new Query();
		query.addCriteria(Criteria.where("breakfastmenu.style").is("STY_1.0")).fields().include("breakfastmenu.id");
		return mongoTemplate.findOne(query, String.class, MONGO_DOC);
	}

	/**
	 * This will return all Value elements (however there is only one).
	 * 
	 * @return
	 */
	public String doCollectTwo() {
		Query query = new Query();
		query.addCriteria(Criteria.where("breakfastmenu.style").is("STY_1.0")).fields()
				.include("breakfastmenu.food.name");
		return mongoTemplate.findOne(query, String.class, MONGO_DOC);
	}

	/**
	 * Searches for breakfastmenu with specific id and status date. includes
	 * only fields title and description within Value element.
	 * 
	 * @return
	 */
	public String doCollectThree() {
		Query query = new Query();
		query.addCriteria(
				Criteria.where("breakfastmenu.id").is("NRD-0").and("breakfastmenu.status.date").is("2017-10-18"))
				.fields().include("breakfastmenu.food.name").include("breakfastmenu.food.description");
		return mongoTemplate.findOne(query, String.class, MONGO_DOC);
	}

	/**
	 * Takes a parameter of xml path and returns json as a string
	 * 
	 * @param xmlDocPath
	 * @return
	 * @throws JSONException
	 * @throws IOException
	 */
	private String processXML2JSON(Path xmlDocPath) throws JSONException, IOException {
		String XML_STRING = Files.lines(xmlDocPath).collect(Collectors.joining(NEW_LINE));
		return XML.toJSONObject(XML_STRING).toString(PRETTY_PRINT_INDENT_FACTOR);
	}

	/**
	 * 
	 * @return
	 */
	private String getFilePath() {
		return new File(getClass().getClassLoader().getResource(FILE_NAME).getFile()).getAbsolutePath();
	}

}
