package org.jboss.ddoyle.drools.demo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.kie.scanner.MavenRepository.getMavenRepository;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.drools.compiler.kie.builder.impl.InternalKieModule;
import org.drools.core.ClockType;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.drools.core.util.FileManager;
import org.jboss.ddoyle.drools.demo.listener.RulesFiredAgendaEventListener;
import org.jboss.ddoyle.drools.demo.model.v1.Event;
import org.jboss.ddoyle.drools.demo.model.v1.SimpleEvent;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieScanner;
import org.kie.api.builder.ReleaseId;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.scanner.AbstractKieCiTest;
import org.kie.scanner.MavenRepository;

/**
 * Tests incremental updates of rules in a running KieSession.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class KieSessionRulesUpdateTest extends AbstractKieCiTest {

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd:HHmmssSSS");

	private FileManager fileManager;
	private File kPom;
	private ReleaseId releaseId;

	@Before
	public void setUp() throws Exception {
		this.fileManager = new FileManager();
		this.fileManager.setUp();
		releaseId = KieServices.Factory.get().newReleaseId("org.kie", "scanner-test", "1.0-SNAPSHOT");
		kPom = createKPom(releaseId);
	}

	@Test
	public void testWithOriginalRules() throws Exception {
		KieServices ks = KieServices.Factory.get();
		MavenRepository repository = getMavenRepository();

		InternalKieModule kJar1 = createOriginalRulesKieJar(ks, releaseId);
		repository.deployArtifact(releaseId, kJar1, kPom);

		KieContainer kieContainer = ks.newKieContainer(releaseId);

		KieScanner scanner = ks.newKieScanner(kieContainer);

		// Create the KieSession.
		KieSessionConfiguration kieSessionConfig = createKieSessionConfiguration(ks);
		KieSession kieSession = kieContainer.newKieSession(kieSessionConfig);
		RulesFiredAgendaEventListener rulesFiredListener = new RulesFiredAgendaEventListener();
		kieSession.addEventListener(rulesFiredListener);

		List<SimpleEvent> firstEvents = getFirstSimpleEvents();
		for (SimpleEvent nextEvent : firstEvents) {
			insertAndAdvance(kieSession, nextEvent);
			kieSession.fireAllRules();
		}
		// Assert
		assertEquals(2, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-One"));
		assertEquals(0, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-Two"));

		List<SimpleEvent> secondEvents = getSecondSimpleEvents();
		for (SimpleEvent nextEvent : secondEvents) {
			insertAndAdvance(kieSession, nextEvent);
			kieSession.fireAllRules();
		}

		assertEquals(3, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-One"));
		assertEquals(1, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-Two"));

		kieSession.dispose();
	}

	@Test
	public void testWithAddedRules() throws Exception {
		KieServices ks = KieServices.Factory.get();
		MavenRepository repository = getMavenRepository();

		InternalKieModule kJar1 = createOriginalRulesKieJar(ks, releaseId);
		repository.deployArtifact(releaseId, kJar1, kPom);

		KieContainer kieContainer = ks.newKieContainer(releaseId);

		KieScanner scanner = ks.newKieScanner(kieContainer);

		KieSessionConfiguration kieSessionConfig = createKieSessionConfiguration(ks);
		KieSession kieSession = kieContainer.newKieSession(kieSessionConfig);
		RulesFiredAgendaEventListener rulesFiredListener = new RulesFiredAgendaEventListener();
		kieSession.addEventListener(rulesFiredListener);

		List<SimpleEvent> firstEvents = getFirstSimpleEvents();
		for (SimpleEvent nextEvent : firstEvents) {
			insertAndAdvance(kieSession, nextEvent);
			kieSession.fireAllRules();
		}
		// Assert
		assertEquals(2, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-One"));
		assertEquals(0, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-Two"));

		InternalKieModule kJar2 = createAddedRulesKieJar(ks, releaseId);
		repository.deployArtifact(releaseId, kJar2, kPom);
		// Incrementally update the KieBase and KieSession.
		scanner.scanNow();

		List<SimpleEvent> secondEvents = getSecondSimpleEvents();
		for (SimpleEvent nextEvent : secondEvents) {
			insertAndAdvance(kieSession, nextEvent);
			kieSession.fireAllRules();
		}

		assertEquals(3, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-One"));
		assertEquals(1, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-Two"));
		// This rule will fire for ALL EVENTS in the session. Not only the newly inserted ones.
		assertEquals(3, rulesFiredListener.getNrOfRulesFired("org.jboss.ddoyle.drools.cep.sample" + "-" + "SimpleTestRule-Three"));

		kieSession.dispose();
	}

	private List<SimpleEvent> getFirstSimpleEvents() throws Exception {
		List<SimpleEvent> simpleEvents = new ArrayList<>();
		simpleEvents.add(new SimpleEvent("1", DATE_FORMAT.parse("20150223:090000000")));
		simpleEvents.add(new SimpleEvent("2", DATE_FORMAT.parse("20150223:090005000")));
		return simpleEvents;
	}

	private List<SimpleEvent> getSecondSimpleEvents() throws Exception {
		List<SimpleEvent> simpleEvents = new ArrayList<>();
		simpleEvents.add(new SimpleEvent("3", DATE_FORMAT.parse("20150223:090021000")));
		return simpleEvents;
	}

	/**
	 * Creates the original KJAR.
	 * 
	 * @param ks
	 * @param releaseId
	 * @return
	 */
	private InternalKieModule createOriginalRulesKieJar(KieServices ks, ReleaseId releaseId) {
		List<Resource> resources = new ArrayList<>();
		resources.add(ks.getResources().newClassPathResource("originalRules/rules.drl"));
		return createKieJar(ks, releaseId, resources);
	}

	/**
	 * Creates the KJAR with added rules.
	 * 
	 * @param ks
	 * @param releaseId
	 * @return
	 */
	private InternalKieModule createAddedRulesKieJar(KieServices ks, ReleaseId releaseId) {
		List<Resource> resources = new ArrayList<>();
		resources.add(ks.getResources().newClassPathResource("addedRules/rules.drl"));
		return createKieJar(ks, releaseId, resources);
	}

	/**
	 * Creates the KJAR with deleted rules.
	 * 
	 * @param ks
	 * @param releaseId
	 * @return
	 */
	private InternalKieModule createDeletedRulesKieJar(KieServices ks, ReleaseId releaseId) {
		List<Resource> resources = new ArrayList<>();
		resources.add(ks.getResources().newClassPathResource("deletedRules/rules.drl"));
		return createKieJar(ks, releaseId, resources);
	}

	/**
	 * Creates the KJAR with renamed rules.
	 * 
	 * @param ks
	 * @param releaseId
	 * @return
	 */
	private InternalKieModule createRenamedRulesKieJar(KieServices ks, ReleaseId releaseId) {
		List<Resource> resources = new ArrayList<>();
		resources.add(ks.getResources().newClassPathResource("renamedRules/rules.drl"));
		return createKieJar(ks, releaseId, resources);
	}

	private InternalKieModule createKieJar(KieServices ks, ReleaseId releaseId, List<Resource> resources) {
		KieFileSystem kfs = createKieFileSystemWithKProject(ks, true);
		kfs.writePomXML(getPom(releaseId));
		int counter = 0;
		for (Resource nextResource : resources) {
			String sourcePath = nextResource.getSourcePath();
			// kfs.write("src/main/resources" + nextResource.getSourcePath(), nextResource);
			kfs.write("src/main/resources/rules" + counter + ".drl", nextResource);
			counter++;
		}

		KieBuilder kieBuilder = ks.newKieBuilder(kfs);

		assertTrue("", kieBuilder.buildAll().getResults().getMessages().isEmpty());
		return (InternalKieModule) kieBuilder.getKieModule();
	}

	private File createKPom(ReleaseId releaseId) throws IOException {
		File pomFile = fileManager.newFile("pom.xml");
		fileManager.write(pomFile, getPom(releaseId));
		return pomFile;
	}

	private KieSessionConfiguration createKieSessionConfiguration(KieServices ks) {
		KieSessionConfiguration config = ks.newKieSessionConfiguration();
		config.setProperty(ClockTypeOption.PROPERTY_NAME, ClockType.PSEUDO_CLOCK.getId());
		return config;
	}

	public static void insertAndAdvance(KieSession kieSession, Event event) {
		kieSession.insert(event);
		// Advance the clock if required.
		PseudoClockScheduler clock = kieSession.getSessionClock();
		long advanceTime = event.getTimestamp().getTime() - clock.getCurrentTime();
		if (advanceTime > 0) {
			clock.advanceTime(advanceTime, TimeUnit.MILLISECONDS);
		}
	}

}
