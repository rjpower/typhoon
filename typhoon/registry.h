#ifndef TYPHOON_REGISTRY_H
#define TYPHOON_REGISTRY_H

#include <map>
#include <string>
#include <typeinfo>
#include <grpc++/grpc++.h>

template<class T>
class Registry {
public:
  struct Creator {
    virtual T* create() = 0;
  };

  typedef std::map<std::string, Creator*> Map;

  static void put(const std::string& k, Creator* c) {
    Map& m = getMap();
    m[k] = c;
  }

  static T* create(const std::string& k) {
    Map& m = getMap();
    assert(m.find(k) != m.end());
    return m[k]->create();
  }

  static Map& getMap() {
    if (!m_) {
      m_ = new Map;
    }
    return *m_;
  }
private:
  static Map *m_;
};

template<class T>
typename Registry<T>::Map* Registry<T>::m_;

template<class Base, class Derived>
struct RegistryHelper : public Registry<Base>::Creator {
  RegistryHelper() = delete;

  RegistryHelper(const std::string& k) {
    gpr_log(GPR_INFO, "Registering: %s %s", typeid(Base).name(), k.c_str());
    Registry<Base>::put(k, this);
  }

  Base* create() {
    return new Derived;
  }
};

#define REGISTER(Base, Derived)\
  static RegistryHelper<Base, Derived> _helper_ ## Derived(#Derived);

#endif
