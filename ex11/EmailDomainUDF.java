import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class EmailDomainUDF extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        try {
            String email = (String) input.get(0);
            // Extract the domain part of the email
            return email.split("@")[1];
        } catch (Exception e) {
            // Return null if any exception occurs
            return null;
        }
    }
}

