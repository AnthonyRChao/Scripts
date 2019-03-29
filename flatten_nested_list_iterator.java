/*
 * Bhavya Shah's solution to Leetcode 341.
 */

public class NestedIterator implements Iterator<Integer> {

  private List<NestedInteger> nestedList = null;
  private Queue<NestedInteger> nextQueue = new LinkedList<>();
  
  public NestedIterator(List<NestedInteger> nestedList) {
    this.nestedList = nestedList;
    initiateQueue();
  }
  
  private void initiateQueue() {
    Iterator<NestedInteger> mainListItr = nestedList.iterator();
    while(mainListItr.hasNext()) {
      NestedInteger nextItem = mainListItr.next();
      if(nextItem.getInteger() != null) {
      nextQueue.add(nextItem);
      } else if(nextItem.getList() != null) {
        populateQueue(nextItem.getList());
      }
    }
  }

  private void populateQueue(List<NestedInteger> list) {
    Iterator<NestedInteger> nestedIntegerIterator = list.iterator();
    while(nestedIntegerIterator.hasNext()) {
      NestedInteger nestedInteger = nestedIntegerIterator.next();
      if(nestedInteger.getInteger() != null) {
        nextQueue.offer(nestedInteger);
      } else {
        populateQueue(nestedInteger.getList());
      }
    }
  }

  @Override
  public Integer next() {
    return nextQueue.poll().getInteger();
  }

  @Override
  public boolean hasNext() {
    return !nextQueue.isEmpty();
  }
}
