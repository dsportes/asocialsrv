class Personne:
    def __init__(self, nom):
        self.nom = nom
        self.prenom = "Martin"
    def __str__(self):
        """Méthode appelée lors d'une conversion de l'objet en chaîne"""
        return "{0} {1}".format(self.prenom, self.nom)


class AgentSpecial(Personne):
    """Classe définissant un agent spécial.
    Elle hérite de la classe Personne"""

    def __init__(self, nom, matricule):

        """Un agent se définit par son nom et son matricule"""

        # On appelle explicitement le constructeur de Personne :

        Personne.__init__(self, nom)

        self.matricule = matricule

    def __str__(self):

        """Méthode appelée lors d'une conversion de l'objet en chaîne"""

        return "Agent {0}, matricule {1}".format(self.nom, self.matricule)

p = Personne("toto")
print(str(p))
a = AgentSpecial("titi", "999")
print(str(a))
