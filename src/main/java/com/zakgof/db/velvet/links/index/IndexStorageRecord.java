package com.zakgof.db.velvet.links.index;

import java.util.ArrayList;
import java.util.List;

import com.zakgof.db.velvet.AutoKeyed;

class IndexStorageRecord extends AutoKeyed {
  List<Integer> indices = new ArrayList<Integer>();
}