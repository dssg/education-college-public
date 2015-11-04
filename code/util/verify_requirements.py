import config
import pkg_resources
from pkg_resources import DistributionNotFound, VersionConflict

# dependencies can be any iterable with strings, 
# e.g. file line-by-line iterator
with open (config.PERSISTENCE_PATH + "/requirements.txt", "r") as myfile:
    dependencies=[x.replace('\n', '') for x in myfile.readlines()]


# here, if a dependency is not met, a DistributionNotFound or VersionConflict
# exception is thrown. 
pkg_resources.require(dependencies)