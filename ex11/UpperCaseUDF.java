import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import org.apache.pig.piggybank.evaluation.string.UPPER;  // Import Piggybank's UPPER function

public class UpperCaseUDF extends EvalFunc<String> {

    // Instantiate Piggybank's UPPER function
    private UPPER upperFunc = new UPPER();
    
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        try {
            // Use the UPPER function from Piggybank
            return upperFunc.exec(input);  // Convert to uppercase
        } catch (Exception e) {
            return null;
        }
    }
}
