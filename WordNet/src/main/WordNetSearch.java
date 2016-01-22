package main;

import java.util.HashMap;
import java.util.Map;

import edu.smu.tspell.wordnet.NounSynset;
import edu.smu.tspell.wordnet.Synset;
import edu.smu.tspell.wordnet.SynsetType;
import edu.smu.tspell.wordnet.WordNetDatabase;
import edu.smu.tspell.wordnet.WordSense;

public class WordNetSearch {
	private static WordNetDatabase database = null;
	
	public RelationType GetRelationType(String a, String b)
	{
		if(database == null){
			//locating the wordnet folder
			System.setProperty("wordnet.database.dir", "C:\\Users\\dbkurs16\\Desktop\\WordNet\\2.1\\dict\\");
			
			//getting a database instance
			database = WordNetDatabase.getFileInstance();
		}
		
		//we save the synsets together with their types in this map
		Map<SynsetType, Synset[]> TypedSynsets = new HashMap<SynsetType, Synset[]>();
		for(int i = 0; i < SynsetType.ALL_TYPES.length; i++){
			TypedSynsets.put(SynsetType.ALL_TYPES[i], database.getSynsets(a, SynsetType.ALL_TYPES[i]));
			Synset[] synsets = database.getSynsets(a, SynsetType.ALL_TYPES[i]);
			for (int j = 0; j < synsets.length; j++) {
				String WordForms[] = synsets[j].getWordForms();
				//figuring out the relation between "a" and "b"
				for (int j2 = 0; j2 < WordForms.length; j2++) {
					
					if(WordForms[j2].equals(b))
						return RelationType.SYNONYM;					
					
					WordSense[] antoArray = synsets[j].getAntonyms(WordForms[j2]);
					for (int k = 0; k < antoArray.length; k++) {
						if(antoArray[k].equals(b))
							return RelationType.ANTONYM;
					}				
					
					if(synsets[j].getType().equals(SynsetType.NOUN))
					{
						NounSynset nounSet = (NounSynset)synsets[j];
						NounSynset[] hypoArray = nounSet.getHyponyms();
						NounSynset[] hyperArray = nounSet.getHypernyms();
						
						for (int k = 0; k < hypoArray.length; k++) {
							if(hypoArray[k].equals(b))
								return RelationType.HYPONYM;
						}
						
						for (int k = 0; k < hyperArray.length; k++) {
							if(hyperArray[k].equals(b))
								return RelationType.HYPERNYM;
						}
					}
				}
			}
		}
		
		//if no relation found, we return NO_RELATION
		return RelationType.NO_RELATION;
	}
}
