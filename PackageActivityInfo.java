package dataflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

/* Simple class representing metadata about a package's pickup or
 * dropoff. Is meant to be read from a log line. 
 */
@DefaultCoder(AvroCoder.class)
public class PackageActivityInfo {
	boolean isArrival; // If true, it's being dropped of at a factory
	String location; // Two letter code for factory id or "CU" for customer
	long time; // Time of the drop off or pickup
	int truckId; // ID of the truck doing the drop off or pick up.
	String packageId; // ID of the package in transit

	@Override
	public String toString() {
		return "PackageActivityInfo [isArrival=" + isArrival + ", location=" + location + ", packageId=" + packageId + "]";
	}

	public boolean isArrival() {
		return isArrival;
	}

	public String getLocation() {
		return location;
	}

	public long getTime() {
		return time;
	}

	public int getTruckId() {
		return truckId;
	}

	public String getPackageId() {
		return packageId;
	}

	// Return null if there is any error parsing.
	// Logline: "0, AN, 1467394122, 423, 372A3SZ4J98"
	public static PackageActivityInfo Parse(String logLine) {
		try {
			PackageActivityInfo pickup = new PackageActivityInfo();
			String[] pieces = logLine.split(",");
			if (pieces.length != 5)
				return null;
			int isArrivalInt = Integer.parseInt(pieces[0].trim());
			if (isArrivalInt == 0) {
				pickup.isArrival = false;
			} else if (isArrivalInt == 1) {
				pickup.isArrival = true;
			} else {
				return null;
			}
			pickup.location = pieces[1].trim();
			pickup.time = Long.parseLong(pieces[2].trim());
			pickup.truckId = Integer.parseInt(pieces[3].trim());
			pickup.packageId = pieces[4].trim();
			return pickup;

		} catch (Exception e) {
			return null; // Return null if any error parsing.
		}
	}

}