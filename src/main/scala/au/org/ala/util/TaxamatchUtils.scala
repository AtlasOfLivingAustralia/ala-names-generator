package au.org.ala.util
/**
 * Utilities required to perform some of the taxamatch.  We are only
 * using the sound expressions to associate species.
 *  
 * Originally copied from the NSL taxamatch:
 * 
 * https://ala-nsl.googlecode.com/svn/taxamatch/trunk/
 */
object TaxamatchUtils {
  
  def main(args:Array[String]){
    println(treatWord("Macropus","genus"))
    println(treatWord("rufus","species"))
    println(treatWord("rufus","genus"))    
  }
  
  def treatWord(str2:String, wordType:String):String= {
        var startLetter:Char= str2.head
        var temp = normalize(str2);
        // Do some selective replacement on the leading letter/s only:
        
        if (temp.startsWith("AE")) {
            temp = "E" + temp.substring(2);
        }
        else if (temp.startsWith("CN")) {
            temp = "N" + temp.substring(2);
        }
        else if (temp.startsWith("CT")) {
            temp = "T" + temp.substring(2);
        }
        else if (temp.startsWith("CZ")) {
            temp = "C" + temp.substring(2);
        }
        else if (temp.startsWith("DJ")) {
            temp = "J" + temp.substring(2);
        }
        else if (temp.startsWith("EA")) {
            temp = "E" + temp.substring(2);
        }
        else if (temp.startsWith("EU")) {
            temp = "U" + temp.substring(2);
        }
        else if (temp.startsWith("GN")) {
            temp = "N" + temp.substring(2);
        }
        else if (temp.startsWith("KN")) {
            temp = "N" + temp.substring(2);
        }
        else if (temp.startsWith("MC")) {
            temp = "MAC" + temp.substring(2);
        }
        else if (temp.startsWith("MN")) {
            temp = "N" + temp.substring(2);
        }
        else if (temp.startsWith("OE")) {
            temp = "E" + temp.substring(2);
        }
        else if (temp.startsWith("QU")) {
            temp = "Q" + temp.substring(2);
        }
        else if (temp.startsWith("PS")) {
            temp = "S" + temp.substring(2);
        }
        else if (temp.startsWith("PT")) {
            temp = "T" + temp.substring(2);
        }
        else if (temp.startsWith("TS")) {
            temp = "S" + temp.substring(2);
        }
        else if (temp.startsWith("WR")) {
            temp = "R" + temp.substring(2);
        }
        else if (temp.startsWith("X")) {
            temp = "Z" + temp.substring(2);
        }
        // Now keep the leading character, then do selected "soundalike" replacements. The
        // following letters are equated: AE, OE, E, U, Y and I; IA and A are equated;
        // K and C; Z and S; and H is dropped. Also, A and O are equated, MAC and MC are equated, and SC and S.
        startLetter = temp.charAt(0); // quarantine the leading letter
        temp = temp.substring(1); // snip off the leading letter
        // now do the replacements
        temp = temp.replaceAll("AE", "I");
        temp = temp.replaceAll("IA", "A");
        temp = temp.replaceAll("OE", "I");
        temp = temp.replaceAll("OI", "A");
        temp = temp.replaceAll("SC", "S");
        temp = temp.replaceAll("E", "I");
        temp = temp.replaceAll("O", "A");
        temp = temp.replaceAll("U", "I");
        temp = temp.replaceAll("Y", "I");
        temp = temp.replaceAll("K", "C");
        temp = temp.replaceAll("Z", "C");
        temp = temp.replaceAll("H", "");
        // add back the leading letter
        temp = startLetter + temp;
        // now drop any repeated characters (AA becomes A, BB or BBB becomes B, etc.)
        temp = temp.replaceAll("(\\w)\\1+", "$1");

        if (wordType == "species") {
            if (temp.endsWith("IS")) {
                temp = temp.substring(0, temp.length() - 2) + "A";
            }
            else if (temp.endsWith("IM")) {
                temp = temp.substring(0, temp.length() - 2) + "A";
            }
            else if (temp.endsWith("AS")) {
                temp = temp.substring(0, temp.length() - 2) + "A";
            }
            //temp = temp.replaceAll("(\\w)\\1+", "$1");
        }

        return temp;
    }
  
    def normalize(str:String) :String = {
        
        if (str == null) return null;
        
        
        
        // trim any leading, trailing spaces or line feeds
        var output = str.trim;
        
        output = output.replace(" cf ", " ");
        output = output.replace(" cf. ", " ");
        output = output.replace(" near ", " ");
        output = output.replace(" aff. ", " ");
        output = output.replace(" sp.", " ");
        output = output.replace(" spp.", " ");
        output = output.replace(" spp ", " ");

        output = str.toUpperCase();
        
        // replace any HTML ampersands
        output = output.replace(" &AMP; ", " & ");
        
        // remove any content in angle brackets (e.g. html tags - <i>, </i>, etc.)
        output = output.replaceAll("\\<.+?\\>", "");
        
        output = translate(output,"\u00c1\u00c9\u00cd\u00d3\u00da\u00c0\u00c8\u00cc\u00d2\u00d9" +
                "\u00c2\u00ca\u00ce\u00d4\u00db\u00c4\u00cb\u00cf\u00d6\u00dc\u00c3\u00d1\u00d5" +
                "\u00c5\u00c7\u00d8", "AEIOUAEIOUAEIOUAEIOUANOACO");

        output = output.replace("\u00c6", "AE");
        output = output.replaceAll("[^a-zA-Z .]", "");

        return output;
    }
    
    
    
    def translate(source:String, transSource:String, transTarget:String) :String ={
        var result = source
        var tt = transTarget
        
        while (transSource.length() > tt.length()) {
            
            tt += " ";
            
        }
        val trans = transSource.toCharArray() zip tt.toCharArray()
        trans.foreach{case (s,t) =>
          result = result.replace(s,t)
          }
        return result;
    }

}