# EmailDomain.py

@outputSchema("domain:chararray")
def extract_domain(email):
    try:
        return email.split('@')[1]
    except:
        return None
