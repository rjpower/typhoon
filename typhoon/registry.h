#ifndef TYPHOON_REGISTRY_H
#define TYPHOON_REGISTRY_H

template<class T>
class Registry {
public:
  struct Creator {
    virtual T* create();
  };

  typedef std::map<std::string, Creator*> Map;

  static void put(const std::string& k, Creator* c) {
    getMap()[k] = c;
  }

  static T* create(const std::string& k) {
    return getMap()[k]->create();
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

template<class T>
struct RegistryHelper : public Registry<T>::Creator {
  RegistryHelper(const std::string& k) {
    Registry<T>::put(k, this);
  }

  T* create() {
    return new T;
  }
};

#define REGISTER(T, name, klass)\
  static RegistryHelper<T> _helper_ ## klass(#name);

#endif
