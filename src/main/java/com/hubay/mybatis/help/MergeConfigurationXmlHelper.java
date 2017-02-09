package com.hubay.mybatis.help;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import com.hubay.lang.helper.DemonPredict;

/**
 * @author shuye
 * @time 2013-11-8
 */
@SuppressWarnings("unchecked")
public final class MergeConfigurationXmlHelper {

	private final static Logger log = LoggerFactory.getLogger(MergeConfigurationXmlHelper.class);

	private MergeConfigurationXmlHelper() {
	}

	/**
	 * @param configLocations
	 * @return
	 * @throws DocumentException
	 * @throws IOException
	 */
	public static Document merge(Resource[] configLocations) throws DocumentException, IOException {
		DemonPredict.isTrue(configLocations.length > 0, "mybatis configuration must be seted");
		SAXReader saxReader = new SAXReader();
		if (configLocations.length == 1) {
			return saxReader.read(configLocations[0].getFile());
		}
		Document parentDocument = saxReader.read(configLocations[0].getFile());
		for (int i = 1; i < configLocations.length; i++) {
			Document son = saxReader.read(configLocations[i].getFile());
			mergeProperties(parentDocument, son);
			mergeSettings(parentDocument, son);
			mergeAliases(parentDocument, son);
			mergeTypeHandlers(parentDocument, son);
			mergeObjectFactory(parentDocument, son);
			mergeObjectWrapperFactory(parentDocument, son);
			mergePlugins(parentDocument, son);
			mergeEnvironments(parentDocument, son);
			mergeDatabaseIdProvider(parentDocument, son);
			mergeMappers(parentDocument, son);
		}
		if(log.isDebugEnabled()){
			log.info("merge the config xml is \n" + parentDocument.asXML());
		}
		return parentDocument;
	}

	private static void mergeEnvironments(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/environments");
	}

	private static void mergeDatabaseIdProvider(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/databaseIdProvider");
	}

	private static void mergeObjectFactory(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/objectFactory");
	}

	private static void mergeObjectWrapperFactory(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/objectWrapperFactory");
	}

	private static void mergeTypeHandlers(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/typeHandlers");
	}

	private static void mergeSettings(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/settings");
	}

	private static void mergeProperties(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/properties");
	}

	/**
	 * @param parent
	 * @param son
	 */
	private static void mergePlugins(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/plugins");
	}

	/**
	 * @param parent
	 * @param son
	 */
	private static void mergeMappers(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/mappers");
	}

	/**
	 * @param parent
	 * @param son
	 */
	private static void mergeAliases(Document parent, Document son) {
		mergeSupport(parent, son, "/configuration/typeAliases");
	}
	
	/**
	 * @param parent
	 * @param son
	 * @param elementName
	 */
	private static void mergeSupport(Document parent, Document son, String elementName) {
		Element father = (Element) parent.selectSingleNode(elementName);
		List<Element> elements = son.selectNodes(elementName + "/*");
		if (null == father && null != elements && elements.size() > 0)
			father = parent.getRootElement().addElement(StringUtils.substringAfterLast(elementName, "/"));
		if (null != elements) {
			for (Element element : elements) {
				father.add(element.detach());
			}
		}
	}

}
