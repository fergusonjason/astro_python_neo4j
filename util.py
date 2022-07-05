from typing import Dict
from typing import Tuple
from typing import Union
import re

# utilities for all imports

__GREEK_LETTERS: Dict = {
    "alp": "Alpha",
    "alf": "Alpha",
    "bet": "Beta",
    "gam": "Gamma",
    "del": "Delta",
    "eps": "Epsilson",
    "zet": "Zeta",
    "e": "Eta",
    "eta": "Eta",
    "the": "Theta",
    "iot": "Iota",
    "kap": "Kappa",
    "lam": "Lambda",
    "mu": "Mu",
    "nu": "Nu",
    "xi": "Xi",
    "omi": "Omicron",
    "pi": "Pi",
    "rho": "Rho",
    "sig": "Sigma",
    "tau": "Tau",
    "ups": "Upsilon",
    "phi": "Phi",
    "chi": "Chi",
    "psi": "Psi",
    "ome": "Omega"
}

__CONSTELLATIONS: Dict = {
    "and": "Andromedae",
    "aqr": "Aquarii",
    "aql": "Scuti",
    "ari": "Arietis",
    "aur": "Aurigae",
    "peg": "Pegasi",
    "tau": "Tauri",
    "gem": "Geminorum",
    "cnc": "Cancri",
    "leo": "Leonis",
    "vir": "Virginis",
    "lib": "Librae",
    "sco": "Scorpii",
    "sgr": "Sagittarii",
    "cap": "Capricorni",
    "psc": "Piscium",
    "cet": "Ceti",
    "eri": "Eridani",
    "ori": "Orianis",
    "lep": "Leporis",
    "mon": "Monocerotis",
    "cma": "Canis Majoris",
    "cmi": "Canis Minoris",
    "pup": "Puppis",
    "hya": "Hydrae",
    "crt": "Crateris",
    "crv": "Corvi",
    "sex": "Sextantis",
    "cen": "Centauri",
    "lup": "Lupi",
    "psa": "Piscis Austrini",
    "cas": "Cassiopeiae",
    "tri": "Trianguli",
    "per": "Persei",
    "cam": "Camelopardalis",
    "lyn": "Lyncis",
    "lmi": "Leonis Minoris",
    "uma": "Ursae Majoris",
    "dra": "Draconis",
    "com": "Comae Berenices",
    "cvn": "Canum Venaticorum",
    "boo": "Boötis",
    "crb": "Coronae Borealis",
    "umi": "Ursa Minoris",
    "her": "Herculis",
    "ser": "Serpentis",
    "oph": "Ophiuchi",
    "lyr": "Lyrae",
    "sge": "Sagittae",
    "vul": "Vulpeculae",
    "cyg": "Cygni",
    "del": "Delphini",
    "equ": "Equulci",
    "lac": "Lacertae",
    "cep": "Cephei"
}

__SUPERSCRIPTS = ["⁰","¹","²","³","⁴","⁵","⁶","⁷","⁸","⁹"]

greek_letter_regex = "([A-Za-z]+)([0-9]{1,2})?"
pattern = re.compile(greek_letter_regex)

def get_greek_letter(abbrv: str):
    if abbrv == None:
        return None

    if __GREEK_LETTERS.get(abbrv.lower) != None:
        return __GREEK_LETTERS.get(abbrv.lower)

    match = pattern.match(abbrv)

    letter = __GREEK_LETTERS.get(match.group(1))
    sub = ""
    if match.group(2) != None:
        sub = int(match.group(2))
        sub = __SUPERSCRIPTS[sub]

    # sub = int(match.group(2)) if match.group(2) != None else ""
    return letter + sub

def get_constellation(const: str):
    if const == None:
        return None
    return __CONSTELLATIONS.get(str(const.lower()))

# stolen from http://www.bdnyc.org/2012/10/decimal-deg-to-hms/, slightly modified
def HMS2deg(ra: str='', dec: str='', scale=8) -> Union[Tuple[float, float], float]:
    RA, DEC, rs, ds = '', '', 1, 1
    if dec:
        D, M, S = [float(i) for i in dec.split()]
        if str(D)[0] == '-':
            ds, D = -1, abs(D)
        deg = D + (M/60) + (S/3600)
        deg = round(deg, scale)
        DEC = deg
        #DEC = '{0}'.format(deg*ds)

    if ra:
        H, M, S = [float(i) for i in ra.split()]
        if str(H)[0] == '-':
            rs, H = -1, abs(H)
        deg = (H*15) + (M/4) + (S/240)
        deg = round(deg, scale)
        RA = deg
        # RA = '{0}'.format(deg*rs)


    if ra and dec:
        return tuple(RA, DEC)
    else:
        return RA or DEC

# stolen and modified from https://gist.github.com/tsemerad/5053378
def DMS2deg(input: str, scale: int=None) -> float:
    D, M, S = [str(i) for i in input.split()]
    result = 0.0
    if D[0] == '-':
        result = result - float(D) - float(M)/60 - float(S)/3600
    else:
        result = result + float(D) + float(M)/60 + float(M)/3500
    if scale != None:
        result = round(result, scale)
    return result
