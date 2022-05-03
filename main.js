import fs from "fs";
import csv from "csv-parser";
import { getAuth, signInWithEmailAndPassword } from "firebase/auth";
import { initializeApp } from "firebase/app";
import {
  getFirestore,
  collection,
  where,
  query,
  getDocs,
  collectionGroup,
} from "firebase/firestore";
import fetch from "node-fetch";
import constants from "./constants.js";
import stripBom from "strip-bom-stream";

const firebaseConfig = {
  apiKey: constants.apiKey,
  authDomain: constants.authDomain,
  databaseURL: constants.databaseURL,
  projectId: constants.projectId,
  storageBucket: constants.storageBucket,
  messagingSenderId: constants.messagingSenderId,
  appId: constants.appId,
  measurementId: constants.measurementId,
};

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

async function authenticateAcc(email, password) {
  try {
    const auth = getAuth(app);
    const userCredential = await signInWithEmailAndPassword(
      auth,
      email,
      password
    );
    const user = userCredential.user.toJSON();
    return { uid: user.uid, token: user.stsTokenManager.accessToken };
  } catch (err) {
    console.error("Auth error: ", err);
  }
}

async function genLatLong(address) {
  const addr_param = address.join("+");
  try {
    let res = await fetch(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${addr_param}&key=${constants.geocodeApiKey}`
    );
    res = await res.json();
    return res.results[0].geometry.location;
  } catch (err) {
    console.error("Geocoding error: ", err);
  }
}

async function readCsv(file_name) {
  const raw_parcels = [];
  fs.createReadStream(file_name)
    .pipe(stripBom())
    .pipe(csv())
    .on("data", (data) => raw_parcels.push(data))
    .on("end", () => processParcels(raw_parcels));
}

async function registerParcelsOnBE(token, parcels) {
  for (let i = 0; i < parcels.length; i++) {
    try {
      let res = await fetch(constants.orderCreateLink, {
        method: "POST",
        body: JSON.stringify(parcels[i]),
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      });
      if (res.status != 201) {
        throw { status: res.status, err: await res.json() };
      }
      res = await res.json();
      console.log(res);
    } catch ({ status, err }) {
      console.error(`Error at index ${i} with error code ${status}`, err);
    }
  }
  console.log("Finished! Please Ctrl-C.");
}

async function getWarehouses(uid) {
  try {
    const orgUsersRef = collectionGroup(db, "orgUsers");
    const userOrgQuery = query(orgUsersRef, where("userId", "==", uid));
    const userOrgSnapshot = await getDocs(userOrgQuery);
    let orgId = null;
    userOrgSnapshot.forEach((doc) => (orgId = doc.data().orgId));
    const warehouseRef = collection(db, `organizations/${orgId}/warehouses`);
    const warehouseSnapshot = await getDocs(warehouseRef);
    const warehouse = [];
    warehouseSnapshot.forEach((doc) => warehouse.push(doc.data()));
    return warehouse;
  } catch (err) {
    console.error(err);
  }
}

async function genAddrObj(parcel) {
  // Gets longitude and latitude of the desired location
  const { lat, lng } = await genLatLong([
    parcel["apartment(Delivery)"],
    parcel["building(Delivery)"],
    parcel["street(Delivery)"],
    parcel["city(Delivery)"],
    parcel["country(Delivery)"],
  ]);
  return {
    apartment: parcel["apartment(Delivery)"],
    building: parcel["building(Delivery)"],
    street: parcel["street(Delivery)"],
    city: parcel["city(Delivery)"],
    state: parcel["state(Delivery)"],
    country: parcel["country(Delivery)"],
    postcode: parcel["postcode(Delivery)"],
    hint: parcel["note"],
    latitude: lat,
    longitude: lng,
  };
}

function getPickupAddr(parcel, addrs) {
  // Chooses one warehouse from the warehouses from the organization.
  const pickAddr = addrs.find(
    (item) =>
      parcel["apartment(PickUp)"] == item.address.apartment &&
      parcel["building(PickUp)"] == item.address.building &&
      parcel["street(PickUp)"] == item.address.street &&
      parcel["postcode(PickUp)"] == item.address.postcode &&
      parcel["city(PickUp)"] == item.address.city
  );
  if (pickAddr == undefined) {
    throw "Warehouse Address doesn't exist in the organization";
  } else {
    return pickAddr;
  }
}

async function genParcelFromCSVParcels(parcel, addrs) {
  let pick_addr = getPickupAddr(parcel, addrs);
  let ship_addr = await genAddrObj(parcel);
  return {
    pickupType: "Asap",
    referenceNumber1: parcel["referenceNumber1"]
      ? parcel["referenceNumber1"]
      : "",
    referenceNumber2: parcel["referenceNumber2"]
      ? parcel["referenceNumber2"]
      : "",
    receiverData: {
      fullName: parcel["fullName"],
      email: parcel["email"],
      phone: parcel["phone"],
    },
    parcelLocation: {
      pickupWarehouseId: pick_addr.id,
      pickupAddress: pick_addr.address,
      shippingAddress: ship_addr,
    },
    note: parcel["note"],
    parcels: [
      {
        dimension_x: parseInt(parcel["dimension_x"]),
        dimension_y: parseInt(parcel["dimension_y"]),
        dimension_z: parseInt(parcel["dimension_z"]),
        requiresAttention:
          parcel["requiresAttention"].toUpperCase() == "TRUE" ? true : false,
        weight: parseInt(parcel["weight"]),
      },
    ],
  };
}

async function processParcels(csvParcels) {
  const parcels = [];
  const { uid, token } = await authenticateAcc(
    process.argv[2],
    process.argv[3]
  );
  const addrs = await getWarehouses(uid);
  for (let i = 0; i < csvParcels.length; i++) {
    try {
      parcels.push(await genParcelFromCSVParcels(csvParcels[i], addrs));
    } catch (err) {
      console.error(`Error at index ${i}:`, err);
    }
  }
  await registerParcelsOnBE(token, parcels);
}

readCsv(process.argv[4]);
